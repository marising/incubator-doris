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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.InlineViewRef;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Column;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.RowBatch;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Status;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CacheAnalyzer {
    private static final Logger LOG = LogManager.getLogger(CacheAnalyzer.class);

    public enum CacheModel {
        None,
        Sql,
        Partition
    }
    private long stmtId;
    private String sqlKey;
    private CacheModel cacheModel;
    private CacheProxy.FetchCacheResult cacheResult;
    private StatementBase parsedStmt;
    //normal sql
    private SelectStmt selectStmt;
    //no partition key's sql
    private SelectStmt nokeyStmt;
    //rewrite sql
    private SelectStmt rewriteStmt;
    private List<ScanNode> scanNodes;
    private OlapScanNode olapNode;
    private OlapTable olapTable;
    private RangePartitionInfo partitionInfo;
    private Column partColumn;
    private CompoundPredicate partitionKeyPredicate;
    private PartitionRange range;
    private List<RowBatch> rowBatchList;

    public CacheAnalyzer(ConnectContext context, StmtExecutor executor, Analyzer analyzer, Planner planner) {
        parsedStmt = executor.getParsedStmt();
        scanNodes = planner.getScanNodes();
        cacheResult = null;
        stmtId = context.getStmtId();
    }

    public CacheAnalyzer(StatementBase parsedStmt, List<ScanNode> scanNodes) {
        this.parsedStmt = parsedStmt;
        this.scanNodes = scanNodes;
    }

    public CacheModel getCacheModel() {
        return cacheModel;
    }

    public CompoundPredicate getPartitionPredicate() {
        return partitionKeyPredicate;
    }

    public SelectStmt getOrgStmt() {
        return selectStmt;
    }
    
    public SelectStmt getNokeyStmt() {
        return nokeyStmt;
    }
 
    public SelectStmt getRewriteStmt() {
        return rewriteStmt;
    }

    /**
     * Check cache mode with SQL and table
     * 1、Only Olap table
     * 2、The update time of the table is before Config.last_version_interval_time
     * 2、PartitionType is PartitionType.RANGE, and partition key has only one column
     * 4、Partition key must be included in the group by clause
     * 5、Where clause must contain only one partition key predicate
     * CacheModel.Table
     *  xxx FROM user_profile, updated before Config.last_version_interval_time
     * CacheModel.Partition, partition by event_date, only the partition of today will be updated.
     *  SELECT xxx FROM app_event WHERE event_date >= 20191201 AND event_date <= 20191207 GROUP BY event_date
     *  SELECT xxx FROM app_event INNER JOIN user_Profile ON app_event.user_id = user_profile.user_id xxx
     *  SELECT xxx FROM app_event INNER JOIN user_profile ON xxx INNER JOIN site_channel ON xxx
     */
    public CacheModel checkCacheModel(long now) {
        if (!Config.enable_sql_cache && !Config.enable_partition_cache) {
            return CacheModel.None;
        }
        if (!(parsedStmt instanceof SelectStmt) || scanNodes.size() == 0) {
            return CacheModel.None;
        }
        this.selectStmt = (SelectStmt) parsedStmt;
        //Check the last version time of the table
        List<Pair<Long, Integer>> tblTimeList = Lists.newArrayList();
        for (int i = 0; i < scanNodes.size(); i++) {
            ScanNode node = scanNodes.get(i);
            if (!(node instanceof OlapScanNode)) {
                return CacheModel.None;
            }
            OlapScanNode oNode = (OlapScanNode) node;
            OlapTable oTable = oNode.getOlapTable();
            long maxTime = getLastUpdateTime(oTable);
            tblTimeList.add(new Pair<Long, Integer>(maxTime, i));
        }
        Collections.sort(tblTimeList, Collections.reverseOrder());
        if (now == 0) {
            now = nowtime();
        }
        if (Config.enable_sql_cache &&
                (now - tblTimeList.get(0).first) >= Config.last_version_interval_second * 1000) {
            return CacheModel.Sql;
        }

        if (!Config.enable_partition_cache) {
            return CacheModel.None;
        }

        //Check if selectStmt matches partition key
        //Only one table can be updated in Config.last_version_interval_second range
        for (int i = 1; i < tblTimeList.size(); i++) {
            if ((now - tblTimeList.get(i).first) < Config.last_version_interval_second * 1000) {
                return CacheModel.None;
            }
        }
        olapNode = (OlapScanNode) scanNodes.get(tblTimeList.get(0).second);
        olapTable = olapNode.getOlapTable();
        if (olapTable.getPartitionInfo().getType() != PartitionType.RANGE) {
            LOG.debug("The partition of OlapTable not RANGE Type.");
            return CacheModel.None;
        }
        partitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
        List<Column> columns = partitionInfo.getPartitionColumns();
        //Partition key has only one column
        if (columns.size() != 1) {
            LOG.debug("Size of columns for partition key {}", columns.size());
            return CacheModel.None;
        }
        partColumn = columns.get(0);
        //Check if group expr contain partition column
        if (!checkGroupByPartitionKey(this.selectStmt, partColumn)) {
            LOG.debug("Not group by partition key, key={}",partColumn.getName());
            return CacheModel.None;
        }
        //Check if whereClause have one CompoundPredicate of partition column
        List<CompoundPredicate> compoundPredicates = Lists.newArrayList();
        getPartitionKeyFromSelectStmt(this.selectStmt, partColumn, compoundPredicates);
        if (compoundPredicates.size() != 1) {
            LOG.debug("the predicate size include partition key has {}.", compoundPredicates.size());
            return CacheModel.None;
        }
        partitionKeyPredicate = compoundPredicates.get(0);
        return CacheModel.Partition;
    }

    public CacheProxy.FetchCacheResult getCache() {
        cacheResult = null;
        cacheModel = checkCacheModel(0);
        if (cacheModel == CacheModel.None) {
            return cacheResult;
        }
        CachePartition cachePart = CachePartition.getInstance();
        CacheProxy proxy = new CacheProxy();
        CacheProxy.FetchCacheRequest request;
        Status status = new Status();
        if (cacheModel == CacheModel.Sql) {
            request = new CacheProxy.FetchCacheRequest(parsedStmt.toSql());
            cacheResult = proxy.fetchCache(request, 1000, status);
            LOG.info("fetch table cache, msg={}", status.getErrorMsg());
            if (cacheResult != null) {
                MetricRepo.COUNTER_CACHE_SQL.increase(1L);
            }
        } else if (cacheModel == CacheModel.Partition) {
            rewriteSelectStmt(null);
            request = new CacheProxy.FetchCacheRequest(nokeyStmt.toSql());
            range = new PartitionRange(this.partitionKeyPredicate, this.olapTable, 
                                        this.partitionInfo);
            if (!range.analytics()) {
                return cacheResult;
            }
            
            for (PartitionRange.PartitionSingle single : range.getSingleList()) {
                request.addParam(single.getPartitionKey(),
                        single.getPartition().getVisibleVersion(),
                        single.getPartition().getVisibleVersionTime()
                );
            }
            cacheResult = proxy.fetchCache(request, 10000, status);
            LOG.info("fetch partition cache, msg={}", status.getErrorMsg());
            for(CacheProxy.FetchCacheValue value :cacheResult.getValueList()) {
                range.setCacheFlag(value.getPartitionKey());
            }
            List<PartitionRange.PartitionSingle> newRangeList = range.newPartitionRange();
            rewriteSelectStmt(newRangeList);
            if (cacheResult != null) {
                MetricRepo.COUNTER_CACHE_PARTITION.increase(1L);
                MetricRepo.COUNTER_PARTITION_ALL.increase((long) range.getSingleList().size());
                MetricRepo.COUNTER_PARTITION_HIT.increase((long) cacheResult.getValueList().size());
            }
        }

        if( cacheResult != null){
            LOG.info("Hit cache model{}, stmtid{}",cacheModel, stmtId);	 
        } else {
            LOG.info("Miss cache model{}, stmtid{}", cacheModel, stmtId);	 
        }
        return cacheResult;
    }

    //Append rowBatch to list,then updateCache
    public void appendRowBatch(RowBatch rowBatch) {
        rowBatchList.add(rowBatch);
    }

    public void updateCache() {
        if (rowBatchList.size() > Config.cache_result_max_row_count) {
            LOG.info("Can not be cached. Rowbatch size {} is more than {}", rowBatchList.size(),
                    Config.cache_result_max_row_count);
            return;
        }
        MySqlRowBuffer mysqlBuffer = new MySqlRowBuffer(this.selectStmt.getResultExprs(), 
                this.selectStmt.getColLabels(), partColumn);
        for (RowBatch row : rowBatchList) {
            mysqlBuffer.appendRowBatch(row);
        }
        CacheProxy.UpdateCacheRequest updateRequest = mysqlBuffer.getUpdateRequest();
        for (CacheProxy.UpdateCacheValue value : updateRequest.getValueList()) {
            long partKey = value.getPartitionKey();
        }
        CacheProxy proxy = new CacheProxy();
        Status status = new Status();
        proxy.updateCache(updateRequest,status);
        LOG.info("Update cache model{}, stmtid{}, ", cacheModel, stmtId);	 
    }

    public long nowtime() {
        return System.currentTimeMillis();
    }

    /**
    * Set the predicate containing partition key to null
     */
    public void rewriteSelectStmt(List<PartitionRange.PartitionSingle> newRangeList) {
        if (newRangeList == null) {
            this.nokeyStmt = (SelectStmt) this.selectStmt.clone();
            rewriteSelectStmt(nokeyStmt, this.partitionKeyPredicate, null);
        } else {
            this.rewriteStmt = (SelectStmt) this.selectStmt.clone();
            rewriteSelectStmt(rewriteStmt, this.partitionKeyPredicate, newRangeList);
        }
    }

    private void rewriteSelectStmt(SelectStmt newStmt, CompoundPredicate predicate, 
        List<PartitionRange.PartitionSingle> newRangeList) {
        newStmt.setWhereClause(
            rewriteWhereClause(newStmt.getWhereClause(), predicate, newRangeList)
        );
        List<TableRef> tableRefs = newStmt.getTableRefs();
        for (TableRef tblRef : tableRefs) {
            if (tblRef instanceof InlineViewRef) {
                InlineViewRef viewRef = (InlineViewRef) tblRef;
                QueryStmt queryStmt = viewRef.getViewStmt();
                if (queryStmt instanceof SelectStmt) {
                    rewriteSelectStmt((SelectStmt) queryStmt, predicate, newRangeList);
                }
            }
        }
    }

    /**
    * P1 And P2 And P3 And P4
    */
    private Expr rewriteWhereClause(Expr expr, CompoundPredicate predicate, 
        List<PartitionRange.PartitionSingle> newRangeList) {
        if (expr==null) {
            return null;
        }
        if (!(expr instanceof CompoundPredicate)) {
            return expr;
        }
        if (expr.equals(predicate)) {
            if (newRangeList == null) {
                return null;
            } else {                 
                getPartitionRange().rewritePredicate((CompoundPredicate)expr, newRangeList);
                return expr;
            }
        }
        
        for (int i = 0; i < expr.getChildren().size(); i++) {
            Expr child = rewriteWhereClause(expr.getChild(i), predicate, newRangeList); 
            if (child == null) {
                expr.removeNode(i);
                i--;
            } else {
                expr.setChild(i, child);
            }
        }
        if (expr.getChildren().size() == 0) {
            return null;
        } else if (expr.getChildren().size() == 1) {
            return expr.getChild(0); 
        } else {
            return expr;
        }
    }

    private void getPartitionKeyFromSelectStmt(SelectStmt stmt, Column partColumn,
                                                            List<CompoundPredicate> compoundPredicates) {
        getPartitionKeyFromWhereClause(stmt.getWhereClause(), partColumn, compoundPredicates);
        List<TableRef> tableRefs = stmt.getTableRefs();
        for (TableRef tblRef : tableRefs) {
            if (tblRef instanceof InlineViewRef) {
                InlineViewRef viewRef = (InlineViewRef) tblRef;
                QueryStmt queryStmt = viewRef.getViewStmt();
                if (queryStmt instanceof SelectStmt) {
                    getPartitionKeyFromSelectStmt((SelectStmt) stmt, partColumn, compoundPredicates);
                }
            }
        }
    }

    /**
     * Only support case 1
     * 1.key >= a and key <= b
     * 2.key = a or key = b
     * 3.key in(a,b,c)
     */
    private void getPartitionKeyFromWhereClause(Expr expr, Column partColumn,
                                                             List<CompoundPredicate> compoundPredicates) {
        if (expr == null) {
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
                getPartitionKeyFromWhereClause(subExpr, partColumn, compoundPredicates);
            }
        }
    }

    /*
    * Check the selectStmt and tableRefs always group by partition key
    * 1. At least one group by
    * 2. group by must contain partition key
    * 3. AggregateInfo cannot be distinct agg
     */
    private boolean checkGroupByPartitionKey(SelectStmt stmt, Column partColumn) {
        List<AggregateInfo> aggInfoList = Lists.newArrayList();
        getAggInfoList(stmt, aggInfoList);
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

    private void getAggInfoList(SelectStmt stmt, List<AggregateInfo> aggInfoList) {        
        AggregateInfo aggInfo = stmt.getAggInfo();        
        if (aggInfo != null) {
            aggInfoList.add(aggInfo);
        }
        List<TableRef> tableRefs = stmt.getTableRefs();
        for (TableRef tblRef : tableRefs) {
            if (tblRef instanceof InlineViewRef) {
                InlineViewRef viewRef = (InlineViewRef) tblRef;
                QueryStmt queryStmt = viewRef.getViewStmt();
                if (queryStmt instanceof SelectStmt) {
                    getAggInfoList(stmt, aggInfoList);
                }
            }
        }
    }

    private long getLastUpdateTime(OlapTable olapTable) {
        long maxTime = 0;
        for(Partition partition : olapTable.getPartitions()){
            if( partition.getVisibleVersionTime() > maxTime ){
                maxTime = partition.getVisibleVersionTime();
            }
        }
        return maxTime;
    }
    
    public PartitionRange getPartitionRange() {
        if (range == null) {
            range = new PartitionRange(this.partitionKeyPredicate, 
                this.olapTable, this.partitionInfo);
            return range;
        } else {
            return range;
        }
    } 
}
