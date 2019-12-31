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

import com.google.common.collect.Lists;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InlineViewRef;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.qe.RowBatch;
import org.apache.doris.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class PartitionCache extends Cache {
    private static final Logger LOG = LogManager.getLogger(PartitionCache.class);
    private SelectStmt nokeyStmt;
    private SelectStmt rewriteStmt;
    private CompoundPredicate partitionPredicate;
    private OlapTable olapTable;
    private RangePartitionInfo partitionInfo;
    private Column partColumn;

    private PartitionRange range;
    private List<PartitionRange.PartitionSingle> newRangeList;

    public SelectStmt getRewriteStmt() {
        return rewriteStmt;
    }

    public SelectStmt getNokeyStmt() {
        return nokeyStmt;
    }

    public PartitionCache(TUniqueId queryId, SelectStmt selectStmt) {
        super(queryId, selectStmt);
    }

    public void setCacheInfo(CacheAnalyzer.CacheTable latestTable, RangePartitionInfo partitionInfo, Column partColumn,
                             CompoundPredicate partitionPredicate) {
        this.latestTable = latestTable;
        this.olapTable = latestTable.olapTable;
        this.partitionInfo = partitionInfo;
        this.partColumn = partColumn;
        this.partitionPredicate = partitionPredicate;
        this.newRangeList = Lists.newArrayList();
    }

    public CacheProxy.FetchCacheResult getCacheData(Status status) {
        CacheProxy.FetchCacheRequest request;
        rewriteSelectStmt(null);
        request = new CacheBeProxy.FetchCacheRequest(nokeyStmt.toSql());
        range = new PartitionRange(this.partitionPredicate, this.olapTable,
                this.partitionInfo);
        if (!range.analytics()) {
            status.setStatus("analytics range error");
            return null;
        }

        for (PartitionRange.PartitionSingle single : range.getPartitionSingleList()) {
            request.addParam(single.getCacheKey().realValue(),
                    single.getPartition().getVisibleVersion(),
                    single.getPartition().getVisibleVersionTime()
            );
        }

        CacheProxy.FetchCacheResult cacheResult = proxy.fetchCache(request, 10000, status);
        if (status.ok() && cacheResult != null) {
            cacheResult.all_count = range.getPartitionSingleList().size();
            for (CacheBeProxy.CacheValue value : cacheResult.getValueList()) {
                range.setCacheFlag(value.param.partition_key);
            }
            MetricRepo.COUNTER_CACHE_HIT_PARTITION.increase(1L);
            MetricRepo.COUNTER_CACHE_PARTITION_ALL.increase((long) range.getPartitionSingleList().size());
            MetricRepo.COUNTER_CACHE_PARTITION_HIT.increase((long) cacheResult.getValueList().size());
        }

        range.setTooNewByID(latestTable.latestId);
        //build rewrite sql
        this.hitRange = range.diskPartitionRange(newRangeList);
        if (newRangeList != null && newRangeList.size() > 0) {
            rewriteSelectStmt(newRangeList);
        }
        return cacheResult;
    }

    public void copyRowBatch(RowBatch rowBatch) {
        if (rowBatchBuilder == null) {
            rowBatchBuilder = new RowBatchBuilder(CacheAnalyzer.CacheMode.Partition);
            rowBatchBuilder.partitionIndex(selectStmt.getResultExprs(), selectStmt.getColLabels(),
                    partColumn, range.updatePartitionRange());
        }
        rowBatchBuilder.copyRowData(rowBatch);
    }

    public void updateCache() {
        if (!super.checkRowLimit()) {
            return;
        }

        CacheBeProxy.UpdateCacheRequest updateRequest = rowBatchBuilder.buildPartitionUpdateRequest(nokeyStmt.toSql());
        if (updateRequest.value_count > 0) {
            CacheBeProxy proxy = new CacheBeProxy();
            Status status = new Status();
            proxy.updateCache(updateRequest, status);
            LOG.info("update cache model {}, queryid {}, sqlkey {}, value count {}, row count {}, data size {}",
                    CacheAnalyzer.CacheMode.Partition, DebugUtil.printId(queryId),
                    DebugUtil.printId(updateRequest.sql_key),
                    updateRequest.value_count, updateRequest.row_count, updateRequest.data_size);
        }
    }

    /**
     * Set the predicate containing partition key to null
     */
    public void rewriteSelectStmt(List<PartitionRange.PartitionSingle> newRangeList) {
        if (newRangeList == null || newRangeList.size() == 0) {
            this.nokeyStmt = (SelectStmt) this.selectStmt.clone();
            rewriteSelectStmt(nokeyStmt, this.partitionPredicate, null);
        } else {
            this.rewriteStmt = (SelectStmt) this.selectStmt.clone();
            rewriteSelectStmt(rewriteStmt, this.partitionPredicate, newRangeList);
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
        if (expr == null) {
            return null;
        }
        if (!(expr instanceof CompoundPredicate)) {
            return expr;
        }
        if (expr.equals(predicate)) {
            if (newRangeList == null) {
                return null;
            } else {
                getPartitionRange().rewritePredicate((CompoundPredicate) expr, newRangeList);
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

    public PartitionRange getPartitionRange() {
        if (range == null) {
            range = new PartitionRange(this.partitionPredicate,
                    this.olapTable, this.partitionInfo);
            return range;
        } else {
            return range;
        }
    }
}
