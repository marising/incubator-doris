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

import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InlineViewRef;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.RowBatch;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Status;

import com.google.common.collect.Lists;
import org.apache.doris.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Analyze which caching mode a SQL is suitable for
 * 1. T + 1 update is suitable for SQL mode
 * 2. Minute update for partition mode
 */
public class CacheAnalyzer {
    private static final Logger LOG = LogManager.getLogger(CacheAnalyzer.class);

    public enum CacheModel {
        None,
        Sql,
        Partition,
        Aggregate
    }

    private TUniqueId queryId;
    private CacheModel cacheModel;
    private CacheTable latestTable;
    private StatementBase parsedStmt;
    private SelectStmt selectStmt;
    private List<ScanNode> scanNodes;
    private OlapTable olapTable;
    private RangePartitionInfo partitionInfo;
    private Column partColumn;
    private CompoundPredicate partitionPredicate;
    private Cache cache;

    public Cache getCache() {
        return cache;
    }

    public CacheAnalyzer(TUniqueId queryId, StatementBase parsedStmt, Planner planner) {
        this.queryId = queryId;
        this.parsedStmt = parsedStmt;
        scanNodes = planner.getScanNodes();
        latestTable = new CacheTable();
    }

    //for unit test
    public CacheAnalyzer(StatementBase parsedStmt, List<ScanNode> scanNodes) {
        this.parsedStmt = parsedStmt;
        this.scanNodes = scanNodes;
    }

    public CacheModel getCacheModel() {
        return cacheModel;
    }

    public class CacheTable {
        public OlapTable olapTable;
        public long latestId;
        public long latestVersion;
        public long latestTime;

        public CacheTable() {
            olapTable = null;
            latestId = 0;
            latestVersion = 0;
            latestTime = 0;
        }

        public void Debug() {
            LOG.info("table {}, partition id {}, ver {}, time {}", olapTable.getName(),latestId, latestVersion, latestTime);
        }
    }

    /**
     * Check cache mode with SQL and table
     * 1、Only Olap table
     * 2、The update time of the table is before Config.last_version_interval_time
     * 2、PartitionType is PartitionType.RANGE, and partition key has only one column
     * 4、Partition key must be included in the group by clause
     * 5、Where clause must contain only one partition key predicate
     * CacheModel.Sql
     * xxx FROM user_profile, updated before Config.last_version_interval_time
     * CacheModel.Partition, partition by event_date, only the partition of today will be updated.
     * SELECT xxx FROM app_event WHERE event_date >= 20191201 AND event_date <= 20191207 GROUP BY event_date
     * SELECT xxx FROM app_event INNER JOIN user_Profile ON app_event.user_id = user_profile.user_id xxx
     * SELECT xxx FROM app_event INNER JOIN user_profile ON xxx INNER JOIN site_channel ON xxx
     */
    public void checkCacheModel(long now) {
        cacheModel = innerCheckCacheModel(now);
    }

    private CacheModel innerCheckCacheModel(long now) {
        if (!Config.enable_sql_cache && !Config.enable_partition_cache) {
            return CacheModel.None;
        }
        if (!(parsedStmt instanceof SelectStmt) || scanNodes.size() == 0) {
            return CacheModel.None;
        }
        this.selectStmt = (SelectStmt) parsedStmt;
        //Check the last version time of the table
        List<Pair<Long, CacheTable>> tblTimeList = Lists.newArrayList();
        for (int i = 0; i < scanNodes.size(); i++) {
            ScanNode node = scanNodes.get(i);
            if (!(node instanceof OlapScanNode)) {
                return CacheModel.None;
            }
            OlapScanNode oNode = (OlapScanNode) node;
            OlapTable oTable = oNode.getOlapTable();
            CacheTable cTable = getLastUpdateTime(oTable);
            tblTimeList.add(new Pair<Long, CacheTable>(cTable.latestTime, cTable));
        }

        Collections.sort(tblTimeList, Collections.reverseOrder());
        latestTable = tblTimeList.get(0).second;
        latestTable.Debug();

        if (now == 0) {
            now = nowtime();
        }
        if (Config.enable_sql_cache &&
                (now - latestTable.latestTime) >= Config.last_version_interval_second * 1000) {
            cache = new SqlCache(this.queryId, this.selectStmt);
            ((SqlCache) cache).setCacheInfo(this.latestTable);
            return CacheModel.Sql;
        }

        if (!Config.enable_partition_cache) {
            return CacheModel.None;
        }

        //Check if selectStmt matches partition key
        //Only one table can be updated in Config.last_version_interval_second range
        for (int i = 1; i < tblTimeList.size(); i++) {
            if ((now - tblTimeList.get(i).first) < Config.last_version_interval_second * 1000) {
                LOG.info("the time of other tables is newer than {}", Config.last_version_interval_second);
                return CacheModel.None;
            }
        }
        olapTable = latestTable.olapTable;
        if (olapTable.getPartitionInfo().getType() != PartitionType.RANGE) {
            LOG.info("the partition of OlapTable not RANGE type");
            return CacheModel.None;
        }
        partitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
        List<Column> columns = partitionInfo.getPartitionColumns();
        //Partition key has only one column
        if (columns.size() != 1) {
            LOG.info("the size of columns for partition key is {}", columns.size());
            return CacheModel.None;
        }
        partColumn = columns.get(0);
        //Check if group expr contain partition column
        if (!checkGroupByPartitionKey(this.selectStmt, partColumn)) {
            LOG.info("not group by partition key, key {}", partColumn.getName());
            return CacheModel.None;
        }
        //Check if whereClause have one CompoundPredicate of partition column
        List<CompoundPredicate> compoundPredicates = Lists.newArrayList();
        getPartitionKeyFromSelectStmt(this.selectStmt, partColumn, compoundPredicates);
        if (compoundPredicates.size() != 1) {
            LOG.info("the predicate size include partition key has {}", compoundPredicates.size());
            return CacheModel.None;
        }
        partitionPredicate = compoundPredicates.get(0);
        cache = new PartitionCache(this.queryId, this.selectStmt);
        ((PartitionCache) cache).setCacheInfo(this.latestTable, this.partitionInfo, this.partColumn,
                this.partitionPredicate);
        return CacheModel.Partition;
    }

    public CacheBeProxy.FetchCacheResult getCacheData() {
        CacheProxy.FetchCacheResult cacheResult = null;
        cacheModel = innerCheckCacheModel(0);
        LOG.info("check cache model {}, queryid {}", cacheModel, DebugUtil.printId(queryId));
        if (cacheModel == CacheModel.None) {
            return cacheResult;
        }
        Status status = new Status();
        cacheResult = cache.getCacheData(status);

        if (status.ok() && cacheResult != null) {
            LOG.info("hit cache, model {}, queryid {}, value size {}, row count {}, data size {}",
                    cacheModel, DebugUtil.printId(queryId),
                    cacheResult.value_count, cacheResult.row_count, cacheResult.data_size);
        } else {
            LOG.info("miss cache, model {}, queryid {}, code {}, msg {}", cacheModel,
                    DebugUtil.printId(queryId), status.getErrorCode(), status.getErrorMsg());
            cacheResult = null;
        }
        return cacheResult;
    }

    public long nowtime() {
        return System.currentTimeMillis();
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
                    String leftColumn = getColumnName(leftPre);
                    String rightColumn = getColumnName(rightPre);
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

    private String getColumnName(BinaryPredicate predicate) {
        SlotRef slot = null;
        if (predicate.getChild(0) instanceof SlotRef) {
            slot = (SlotRef) predicate.getChild(0);
        } else if (predicate.getChild(0) instanceof CastExpr) {
            CastExpr expr = (CastExpr) predicate.getChild(0);
            if (expr.getChild(0) instanceof SlotRef) {
                slot = (SlotRef) expr.getChild(0);
            }
        }

        if (slot != null) {
            return slot.getColumnName();
        } else {
            LOG.info("predicate {}", predicate.toString());
            for (Expr child1 : predicate.getChildren()) {
                LOG.info("child1 {},sql {}", child1.toString(), child1.toSql());
                for (Expr child2 : child1.getChildren()) {
                    LOG.info("child2 {}, sql {}", child2.toString(), child2.toSql());
                }
            }
        }
        return "";
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

    private CacheTable getLastUpdateTime(OlapTable olapTable) {
        CacheTable table = new CacheTable();
        table.olapTable = olapTable;
        for (Partition partition : olapTable.getPartitions()) {
            if (partition.getVisibleVersionTime() >= table.latestTime &&
                    partition.getVisibleVersion() > table.latestVersion) {
                table.latestId = partition.getId();
                table.latestTime = partition.getVisibleVersionTime();
                table.latestVersion = partition.getVisibleVersion();
            }
        }
        return table;
    }

    public SelectStmt getRewriteStmt() {
        if (cacheModel != CacheModel.Partition) {
            return null;
        }
        return cache.getRewriteStmt();
    }

    public void copyRowBatch(RowBatch rowBatch) {
        if (cacheModel == CacheModel.None) {
            return;
        }
        cache.copyRowBatch(rowBatch);
    }

    public void updateCache() {
        if (cacheModel == CacheModel.None) {
            return;
        }
        cache.updateCache();
    }
}
