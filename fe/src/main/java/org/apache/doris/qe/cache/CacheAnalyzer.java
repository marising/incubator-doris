// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.qe.cache;

import org.apache.doris.analysis.*;
import org.apache.doris.catalog.*;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class CacheAnalyzer {
    private static final Logger LOG = LogManager.getLogger(CacheAnalyzer.class);
    public enum CacheModel{
        None,
        Table,
        Partition
    }
    private CacheModel cacheModel;
    private CachePartition.CacheResult cacheResult;
    private StatementBase parsedStmt;
    private SelectStmt selectStmt;
    private SelectStmt nokeySelectStmt;
    private SelectStmt rewriteSelectStmt;
    private List<ScanNode> scanNodes;
    CompoundPredicate partitionKeyPredicate;

    private boolean isHitCache;

    public boolean getIsHitCache() { return isHitCache; }

    public CacheAnalyzer(ConnectContext context, StmtExecutor executor, Analyzer analyzer, Planner planner) {
        parsedStmt = executor.getParsedStmt();
        scanNodes = planner.getScanNodes();
        cacheResult = null;
    }

    public CachePartition.CacheResult getCache() {
        cacheModel = checkCacheModel();
        String sqlKey;
        if (cacheModel == CacheModel.Table) {
            sqlKey = getMd5(parsedStmt.toSql());
            cacheResult = CachePartition.getInstance().getCacheData(sqlKey);
        } else if (cacheModel == CacheModel.Partition) {
            nokeySelectStmt = (SelectStmt) selectStmt.clone();
            rewriteNoKeySelectStmt(nokeySelectStmt, partitionKeyPredicate);
            sqlKey = getMd5(selectStmt.toSql());
            cacheResult = CachePartition.getInstance().getCacheData(sqlKey);
        }
        return cacheResult;
    }
    /**
     * Types of SQL that can be cached
     * 1、Only Olap table
     * 2、The update time of the table is before Config.last_version_min_delta_time minute
     * 2、PartitionType is PartitionType.RANGE, and partition key has only one column
     * 4、Partition key must be included in the group by clause
     * 5、Where clause must contain only one partition key predicate
     * CacheModel.Table
     *  xxx FROM user_profile, updated before Config.last_version_min_delta_time minute
     * CacheModel.Partition, partition by event_date, only the partition of today will be updated.
     *  SELECT xxx FROM app_event WHERE event_date >= 20191201 AND event_date <= 20191207 GROUP BY event_date
     *  SELECT xxx FROM app_event INNER JOIN user_Profile ON app_event.user_id = user_profile.user_id xxx
     *  SELECT xxx FROM app_event INNER JOIN user_profile ON xxx INNER JOIN site_channel ON xxx
     */
    public CacheModel checkCacheModel() {
        //Only select statement
        if (!(parsedStmt instanceof SelectStmt) || scanNodes.size() == 0) {
            return CacheModel.None;
        }
        //Clone selectStmt, then will rewrite
        selectStmt = (SelectStmt) parsedStmt;
        //Check the last update time of the table
        List<Pair<Long, Integer>> tblTimeList = Lists.newArrayList();
        for (int i = 0; i < scanNodes.size(); i++) {
            ScanNode node = scanNodes.get(i);
            if (!(node instanceof OlapScanNode)) {
                return CacheModel.None;
            }
            OlapScanNode olapNode = (OlapScanNode) node;
            OlapTable olapTable = olapNode.getOlapTable();
            long maxTime = getLastUpdateTime(olapTable);
            tblTimeList.add(new Pair<Long, Integer>(maxTime, i));
        }
        Collections.sort(tblTimeList, Collections.reverseOrder());
        long now = System.currentTimeMillis();
        if ((now - tblTimeList.get(0).first) >= Config.last_version_min_delta_time) {
            return CacheModel.Table;
        }
        //Check if selectStmt matches partition key
        //Only one table can be updated in Config.last_version_min_delta_time range
        for (int i = 1; i < tblTimeList.size(); i++) {
            if ((now - tblTimeList.get(i).first) < Config.last_version_min_delta_time) {
                return CacheModel.None;
            }
        }
        OlapScanNode olapNode = (OlapScanNode) scanNodes.get(tblTimeList.get(0).second);
        OlapTable olapTable = olapNode.getOlapTable();
        if (olapTable.getPartitionInfo().getType() != PartitionType.RANGE) {
            return CacheModel.None;
        }
        RangePartitionInfo partitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
        List<Column> columns = partitionInfo.getPartitionColumns();
        //Partition key has only one column
        if (columns.size() != 1) {
            return CacheModel.None;
        }
        Column partColumn = columns.get(0);
        //Check if group expr contain partition column
        if (!checkGroupByPartitionKey(selectStmt, partColumn)) {
            return CacheModel.None;
        }
        //Check if whereClause have one CompoundPredicate of partition column
        List<CompoundPredicate> compoundPredicates = Lists.newArrayList();
        getPartitionKeyFromSelectStmt(selectStmt, partColumn, compoundPredicates);
        if (compoundPredicates.size() != 1) {
            return CacheModel.None;
        }
        partitionKeyPredicate = compoundPredicates.get(0);
        return CacheModel.Partition;
    }

    private void rewriteScanRangeWhereClause(Expr expr, CompoundPredicate predicate, CompoundPredicate cachePredicate){

    }

    /*
    * Set the predicate containing partition key to null
     */
    private void rewriteNoKeySelectStmt(SelectStmt selectStmt, CompoundPredicate predicate) {
        rewriteNoKeyWhereClause(selectStmt.getWhereClause(), predicate);
        List<TableRef> tableRefs = selectStmt.getTableRefs();
        for (TableRef tblRef : tableRefs) {
            if (tblRef instanceof InlineViewRef) {
                InlineViewRef viewRef = (InlineViewRef) tblRef;
                QueryStmt queryStmt = viewRef.getViewStmt();
                if (queryStmt instanceof SelectStmt) {
                    rewriteNoKeySelectStmt((SelectStmt) selectStmt, predicate);
                }
            }
        }
    }

    private void rewriteNoKeyWhereClause(Expr expr, CompoundPredicate predicate){
        if( expr.equals(predicate)){
            expr.clearChildren();
        }else{
            for(Expr subexpr:expr.getChildren()) {
                rewriteNoKeyWhereClause(subexpr, predicate);
            }
        }
    }



    private void getPartitionKeyFromSelectStmt(SelectStmt selectStmt, Column partColumn,
                                                            List<CompoundPredicate> compoundPredicates) {
        getPartitionKeyFromWhereClause(selectStmt.getWhereClause(), partColumn, compoundPredicates);
        List<TableRef> tableRefs = selectStmt.getTableRefs();
        for (TableRef tblRef : tableRefs) {
            if (tblRef instanceof InlineViewRef) {
                InlineViewRef viewRef = (InlineViewRef) tblRef;
                QueryStmt queryStmt = viewRef.getViewStmt();
                if (queryStmt instanceof SelectStmt) {
                    getPartitionKeyFromSelectStmt((SelectStmt) selectStmt, partColumn, compoundPredicates);
                }
            }
        }
    }

    /* Only support case 1
    * 1.key >= a and key <= b
    * 2.key = a or key = b
    * 3.key in(a,b,c)
     */
    private void getPartitionKeyFromWhereClause(Expr expr, Column partColumn,
                                                             List<CompoundPredicate> compoundPredicates) {
        if(expr == null) {
            return;
        }
        if (expr instanceof CompoundPredicate) {
            CompoundPredicate cp = (CompoundPredicate) expr;
            if (cp.getOp() == CompoundPredicate.Operator.AND) {
                if (cp.getChildren().size() == 2 && cp.getChild(0) instanceof BinaryPredicate &&
                        cp.getChild(1) instanceof BinaryPredicate) {
                    BinaryPredicate leftPre = (BinaryPredicate) cp.getChild(0);
                    BinaryPredicate rightPre = (BinaryPredicate) cp.getChild(1);
                    String leftColumn = ((SlotRef) leftPre.getChild(0)).getColumnName();
                    String rightColumn = ((SlotRef) rightPre.getChild(0)).getColumnName();
                    if (leftColumn.equalsIgnoreCase(partColumn.getName()) &&
                            rightColumn.equalsIgnoreCase(partColumn.getName())) {
                        compoundPredicates.add(cp);
                    }
                }
            }
            for (Expr subExpr : expr.getChildren()) {
                cp = getPartitionKeyFromWhereClause(subExpr, partColumn);
                if (cp != null) {
                    compoundPredicates.add(cp);
                }
            }
        }
    }

    /*
    * Check the selectStmt and tableRefs always group by park key
    * 1. At least one group by
    * 2. group by must contain partition key
    * 3. AggregateInfo cannot be distinct agg
     */
    private boolean checkGroupByPartitionKey(SelectStmt selectStmt, Column partColumn) {
        List<AggregateInfo> aggInfoList = Lists.newArrayList();
        getAggInfoList(selectStmt, aggInfoList);
        int groupbyCount = 0;
        for (AggregateInfo aggInfo : aggInfoList) {
            if (aggInfo.isDistinctAgg()) {
                return false;
            }
            ArrayList<Expr> groupExprs = aggInfo.getGroupingExprs();
            if (groupExprs == null) {
                continue;
            }
            groupbyCount += 1;
            boolean matched = false;
            for (Expr groupExpr : groupExprs) {
                SlotRef slot = (SlotRef) groupExpr;
                if (partColumn.getName().equals(slot.getColumnName())) {
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                return false;
            }
        }
        return groupbyCount > 0 ? true : false;
    }

    private void getAggInfoList(SelectStmt selectStmt, List<AggregateInfo> aggInfoList) {
        AggregateInfo aggInfo = selectStmt.getAggInfo();
        if (aggInfo != null) {
            aggInfoList.add(aggInfo);
        }
        List<TableRef> tableRefs = selectStmt.getTableRefs();
        for (TableRef tblRef : tableRefs) {
            if (tblRef instanceof InlineViewRef) {
                InlineViewRef viewRef = (InlineViewRef) tblRef;
                QueryStmt queryStmt = viewRef.getViewStmt();
                if (queryStmt instanceof SelectStmt) {
                    getAggInfoList((SelectStmt) selectStmt, aggInfoList);
                }
            }
        }
    }

    private long getLastUpdateTime(OlapTable olapTable){
        long maxTime = 0;
        for(Partition partition : olapTable.getPartitions()){
            if( partition.getVisibleVersionTime() > maxTime ){
                maxTime = partition.getVisibleVersionTime();
            }
        }
        return maxTime;
    }

    public String getQueryKey() {
        QueryStmt queryStmt = plannerContext.getQueryStmt();
        String sql = queryStmt.toSql();
        return getMd5(sql);
    }

    private static String getMd5(String sql) {
        String hexStr = "";
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] digest = md5.digest(sql.getBytes("utf-8"));
            BigInteger bigInt = new BigInteger(1,digest);
            hexStr = bigInt.toString(16);
        } catch (Exception e) {
            LOG.warning("getMd5 exception : " + e.getMessage());
        }
        return hexStr;
    }
}
