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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PartitionColumnFilter;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.RowBatch;
import org.apache.doris.qe.StmtExecutor;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class PartitionRange {
    private static final Logger LOG = LogManager.getLogger(org.apache.doris.qe.cache.PartitionRange.class);

    public class PartitionSingle{
        private Partition partition;
        private PartitionKey key;
        private long partitionId;
        private long partitionKey; //Cache Key
        private boolean fromCache;
        public Partition getPartition() {
            return partition;
        }
        public void setPartition(Partition partition) {
            this.partition = partition;
        }
        public PartitionKey getKey() {
            return key;
        }
        public void setKey(PartitionKey key) {
            this.key = key;
        }
        public long getPartitionId() {
            return partitionId;
        }
        public void setPartitionId(long partitionId) {
            this.partitionId = partitionId;
        }
        public long getPartitionKey() {
            return partitionKey;
        }
        public void setPartitionKey(long partitionKey) {
            this.partitionKey = partitionKey;
        }
        public boolean isFromCache() {
            return fromCache;
        }
        public void setFromCache(boolean fromCache) {
            this.fromCache = fromCache;
        }
        public PartitionSingle(){
            this.partitionId = 0;
            this.partitionKey = 0;
            this.fromCache = false;
        }
    }
    private CompoundPredicate partitionKeyPredicate;
    private OlapTable olapTable;
    private RangePartitionInfo rangePartitionInfo;
    private Column partitionColumn;
    //private List<PartitionKey> partitionKeyList;
    private List<PartitionSingle> partitionSingleList;


    public CompoundPredicate getPartitionKeyPredicate() {
        return partitionKeyPredicate;
    }
    public void setPartitionKeyPredicate(CompoundPredicate partitionKeyPredicate) {
        this.partitionKeyPredicate = partitionKeyPredicate;
    }
    public RangePartitionInfo getRangePartitionInfo() {
        return rangePartitionInfo;
    }
    public void setRangePartitionInfo(RangePartitionInfo rangePartitionInfo) {
        this.rangePartitionInfo = rangePartitionInfo;
    }
    public Column getPartitionColumn() {
        return partitionColumn;
    }
    public void setPartitionColumn(Column partitionColumn) {
        this.partitionColumn = partitionColumn;
    }
    public List<PartitionSingle> getSingleList() {
        return partitionSingleList;
    }
    public void setSingleList(List<PartitionSingle> singleList) {
        this.partitionSingleList = singleList;
    }

    public PartitionRange(){
    }
    public PartitionRange(CompoundPredicate partitionKeyPredicate, OlapTable olapTable, RangePartitionInfo rangePartitionInfo){
        this.partitionKeyPredicate = partitionKeyPredicate;
        this.olapTable = olapTable;
        this.rangePartitionInfo = rangePartitionInfo;
    }

    /**
     * analytics PartitionKey and PartitionInfo
     *
     * @return
     */
    public boolean analytics(){
        if (rangePartitionInfo.getPartitionColumns().size() != 1){
            return false;
        }
        partitionColumn = rangePartitionInfo.getPartitionColumns().get(0);
        PartitionColumnFilter filter = createPartitionFilter(this.partitionKeyPredicate, partitionColumn);
        List<PartitionSingle> singleList = getPartitionKeyRange(filter, partitionColumn);
        getTablePartitionList(olapTable, singleList);
        return true;
    }

    public boolean setCacheFlag(long partitionKey){
        for(PartitionSingle single : partitionSingleList) {
            if (single.getPartitionKey() == partitionKey) {
                single.setFromCache(true);
            }
        }
    }

    public CompoundPredicate rewritePartitionPricate(){

    }

    /**
     * Get partition info from SQL Predicate and OlapTable
     * Pair<PartitionID, PartitionKey>
     * PARTITION BY RANGE(`olap_date`)
     * ( PARTITION p20200101 VALUES [("20200101"), ("20200102")),
     * PARTITION p20200102 VALUES [("20200102"), ("20200103")) )
     */
    private void getTablePartitionList(OlapTable table, List<PartitionSingle> singleList){
        //find the partition id from key range
        Map<Long, Range<PartitionKey>>  range =  rangePartitionInfo.getIdToRange();
        for(Map.Entry<Long, Range<PartitionKey>> entry : rangePartitionInfo.getIdToRange().entrySet() ){
            Long partId = entry.getKey();
            for(PartitionSingle single : singleList) {
                if (entry.getValue().contains(single.getKey())) {
                    if( single.getPartitionId() == 0) {
                        single.setPartitionId(partId);
                    }
                }
            }
        }
        for(PartitionSingle single : singleList){
            if( single.getPartitionId() != 0){
                single.setPartition(table.getPartition(single.getPartitionId()));
                partitionSingleList.add(single);
            }
        }
    }

    /**
     * Get value range of partition column from predicate
     */
    private List<PartitionSingle> getPartitionKeyRange(PartitionColumnFilter partitionColumnFilter,
                                                       Column partitionColumn) {
        List<PartitionSingle> partitionSingleList = Lists.newArrayList();
        if (partitionColumnFilter.lowerBound == null || partitionColumnFilter.upperBound == null) {
            return partitionSingleList;
        }
        List<Long> partitionKeys = Lists.newArrayList();
        long begin = partitionColumnFilter.lowerBound.getLongValue();
        if (!partitionColumnFilter.lowerBoundInclusive) {
            begin += 1;
        }
        long end = partitionColumnFilter.upperBound.getLongValue();
        if (!partitionColumnFilter.upperBoundInclusive) {
            end -= 1;
        }
        for(int val=begin; val <= end; val++) {
            PartitionKey key = PartitionKey.createPartitionKey(
                    Lists.newArrayList(new PartitionValue(String.valueOf(val)),Lists.newArrayList(partitionColumn)));
            PartitionSingle single = new PartitionSingle();
            single.setPartitionKey(val);
            single.setKey(key);
            partitionSingleList.add(single);
        }
        return partitionSingleList;
    }

    private PartitionColumnFilter createPartitionFilter(CompoundPredicate partitionKeyPredicate,
                                                        Column partitionColumn) {
        if( partitionKeyPredicate.getOp() != CompoundPredicate.Operator.AND ){
            return null;
        }
        PartitionColumnFilter partitionColumnFilter = new PartitionColumnFilter();;
        for (Expr expr : partitionKeyPredicate.getChildren()) {
            if (expr instanceof BinaryPredicate) {
                BinaryPredicate binPredicate = (BinaryPredicate) expr;
                Expr slotBinding = binPredicate.getSlotBinding(desc.getId());
                if (binPredicate.getOp() == BinaryPredicate.Operator.NE
                        || !(slotBinding instanceof LiteralExpr)) {
                    continue;
                }
                LiteralExpr literal = (LiteralExpr) slotBinding;
                BinaryPredicate.Operator op = binPredicate.getOp();
                if (!binPredicate.slotIsLeft()) {
                    op = op.commutative();
                }
                switch (op) {
                    case EQ:
                        partitionColumnFilter.setLowerBound(literal, true);
                        partitionColumnFilter.setUpperBound(literal, true);
                        break;
                    case LE:
                        partitionColumnFilter.setUpperBound(literal, true);
                        partitionColumnFilter.lowerBoundInclusive = true;
                        break;
                    case LT:
                        partitionColumnFilter.setUpperBound(literal, false);
                        partitionColumnFilter.lowerBoundInclusive = true;
                        break;
                    case GE:
                        partitionColumnFilter.setLowerBound(literal, true);
                        break;
                    case GT:
                        partitionColumnFilter.setLowerBound(literal, false);
                        break;
                    default:
                        break;
                }
            } else if (expr instanceof InPredicate) {
                InPredicate inPredicate = (InPredicate) expr;
                if (!inPredicate.isLiteralChildren() || inPredicate.isNotIn()) {
                    continue;
                }
                if (null == partitionColumnFilter) {
                    partitionColumnFilter = new PartitionColumnFilter();
                }
                partitionColumnFilter.setInPredicate(inPredicate);
            }
        }
        return partitionColumnFilter;
    }
}