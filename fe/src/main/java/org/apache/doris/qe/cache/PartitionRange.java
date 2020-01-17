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

import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.planner.PartitionColumnFilter;

import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class PartitionRange {
    private static final Logger LOG = LogManager.getLogger(PartitionRange.class);

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
     * @return
     */
    public boolean analytics(){
        if (rangePartitionInfo.getPartitionColumns().size() != 1){
            return false;
        }
        partitionColumn = rangePartitionInfo.getPartitionColumns().get(0);
        PartitionColumnFilter filter = createPartitionFilter(this.partitionKeyPredicate, partitionColumn);
        try{
            List<PartitionSingle> singleList = getPartitionKeyRange(filter, partitionColumn);
            getTablePartitionList(olapTable, singleList);
        } catch (AnalysisException e) {
            LOG.warn("get partition range failed, because:", e);
            return false;
        }
        return true;
    }

    public boolean setCacheFlag(long partitionKey){
        for(PartitionSingle single : partitionSingleList) {
            if (single.getPartitionKey() == partitionKey) {
                single.setFromCache(true);
            }
        }
        return true;
    }

    public CompoundPredicate rewritePartitionPricate(){
        return null;
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
        Column partitionColumn) throws AnalysisException {
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
        for(long val=begin; val <= end; val++) {
            PartitionKey key = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue(String.valueOf(val))),
                Lists.newArrayList(partitionColumn));
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
                BinaryPredicate.Operator op = binPredicate.getOp();

                Expr slotBinding;
                if (binPredicate.slotIsLeft()) {
                    slotBinding = binPredicate.getChild(0);
                }else{
                    slotBinding = binPredicate.getChild(1);
                }
                
                if (!binPredicate.slotIsLeft()) {
                    op = op.commutative();
                }
                //Expr slotBinding = binPredicate.getSlotBinding(desc.getId());
                if (binPredicate.getOp() == BinaryPredicate.Operator.NE
                        || !(slotBinding instanceof LiteralExpr)) {
                    continue;
                }
                LiteralExpr literal = (LiteralExpr) slotBinding;
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
