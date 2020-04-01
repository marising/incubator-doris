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
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.Type;
import org.apache.doris.planner.PartitionColumnFilter;

import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/*
 * Convert the range of the partition to the list
 */
public class PartitionRange {
    private static final Logger LOG = LogManager.getLogger(PartitionRange.class);

    public class PartitionSingle {
        private Partition partition;
        private PartitionKey key;
        private long partitionId;
        private PartitionKeyType partitionKey; //Cache Key
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

        public PartitionKeyType getPartitionKey() {
            return partitionKey;
        }
        public void setPartitionKey(PartitionKeyType partitionKey) {
            this.partitionKey.clone(partitionKey);
        }

        public boolean isFromCache() {
            return fromCache;
        }
        public void setFromCache(boolean fromCache) {
            this.fromCache = fromCache;
        }
        public PartitionSingle(){
            this.partitionId = 0;
            this.partitionKey = new PartitionKeyType();
            this.fromCache = false;
        }
        public void Debug() {
            LOG.info("partition id:{}, key:{}, version:{}, time:{} ", partitionId, partitionKey.toString(),
                    partition.getVisibleVersion(), partition.getVisibleVersionTime());
        }
    }

    public enum KeyType{
        DEFAULT,
        LONG,
        DATE,
        DATETIME,
        TIME
    }

    public static class PartitionKeyType {
        private SimpleDateFormat df8 = new SimpleDateFormat("yyyyMMdd");
        private SimpleDateFormat df10 = new SimpleDateFormat("yyyy-MM-dd");

        public KeyType keyType = KeyType.DEFAULT;
        public long value;
        public Date date;

        public boolean init(Type type, String str) {
            if (type.getPrimitiveType() == PrimitiveType.DATE) {
                try {
                    date = df10.parse(str);
                } catch (Exception e) {
                    LOG.warn("parse error str{}.", str);
                    return false;
                }
            } else {
                value = Long.valueOf(str);
            }
            return true;
        }

        public boolean init(Type type, LiteralExpr expr) {
            switch (type.getPrimitiveType()) {
                case BOOLEAN:
                case TIME:
                case DATETIME:
                case FLOAT:
                case DOUBLE:
                case DECIMAL:
                case DECIMALV2:
                case CHAR:
                case VARCHAR:
                case LARGEINT:
                    LOG.info("PartitionCache not support such key type {}", type.toSql());
                    return false;
                case DATE:
                    date = getDateValue(expr);
                    keyType = KeyType.DATE;
                    break;
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                    value = expr.getLongValue();
                    keyType = KeyType.LONG;
                    break;
            }
            return true;
        }



        public void clone(PartitionKeyType key) {
            keyType = key.keyType;
            value = key.value;
            date = key.date;
        }

        public boolean equals(PartitionKeyType key) {
            return realValue() == key.realValue();
        }

        public void add(int num) {
            if (keyType == KeyType.DATE) {
                date = new Date(date.getTime() + num * 3600 * 24 * 1000);
            } else {
                value += num;
            }
        }

        public void fromString(){

        }

        public String toString() {
            if (keyType == KeyType.DEFAULT) {
                return "";
            } else if (keyType == KeyType.DATE) {
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                return df.format(date);
            } else {
                return String.valueOf(value);
            }
        }

        public long realValue() {
            if (keyType == KeyType.DATE) {
                SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
                return Long.parseLong(df.format(date));
            } else {
                return value;
            }
        }

        private Date getDateValue(LiteralExpr expr) {
            value = expr.getLongValue() / 1000000;
            Date dt = null;
            try {
                dt = df8.parse(String.valueOf(value));
            } catch (Exception e) {
            }
            return dt;
        }
    }


    private CompoundPredicate partitionKeyPredicate;
    private OlapTable olapTable;
    private RangePartitionInfo rangePartitionInfo;
    private Column partitionColumn;
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

    public PartitionRange() {
    }

    public PartitionRange(CompoundPredicate partitionKeyPredicate, OlapTable olapTable, RangePartitionInfo rangePartitionInfo) {
        this.partitionKeyPredicate = partitionKeyPredicate;
        this.olapTable = olapTable;
        this.rangePartitionInfo = rangePartitionInfo;
        this.partitionSingleList = Lists.newArrayList();
    }

    /**
     * analytics PartitionKey and PartitionInfo
     * @return
     */
    public boolean analytics() {
        if (rangePartitionInfo.getPartitionColumns().size() != 1) {
            return false;
        }
        partitionColumn = rangePartitionInfo.getPartitionColumns().get(0);
        PartitionColumnFilter filter = createPartitionFilter(this.partitionKeyPredicate, partitionColumn);
        try {
            getPartitionKeyRange(filter, partitionColumn);
            getTablePartitionList(olapTable);
        } catch (AnalysisException e) {
            LOG.warn("get partition range failed, because:", e);
            return false;
        }
        return true;
    }

    public boolean setCacheFlag(long partitionKey) {
        for(PartitionSingle single : partitionSingleList) {
            if (single.getPartitionKey().realValue() == partitionKey) {
                single.setFromCache(true);
            }
        }
        return true;
    }

    /**
     * Support left or right hit cache, not support middle.
     * 20200113-2020115, not support 20200114
     */
    public List<PartitionSingle> newPartitionRange() {
        List<PartitionSingle> rangeList = Lists.newArrayList();
        if (partitionSingleList.size() == 0) {
            return rangeList;
        }
        //1 left, 2 right
        int bound = 0;
        int index = 0;
        if (partitionSingleList.get(0).isFromCache()) {
            for (int i = 1; i < partitionSingleList.size()-1; i++) {
                if (partitionSingleList.get(i).isFromCache()) {
                    index = i;
                } else {
                    break;
                }
            }
            rangeList.add(partitionSingleList.get(index+1));
            rangeList.add(partitionSingleList.get(partitionSingleList.size()-1));
        } else if (partitionSingleList.get(partitionSingleList.size()-1).isFromCache()) {
            bound = 2;
            for (int i = partitionSingleList.size()-1; i > 0; i--) {
                if (partitionSingleList.get(i).isFromCache()) {
                    index = i;
                } else {
                    break;
                }
            } 
            rangeList.add(partitionSingleList.get(0));
            rangeList.add(partitionSingleList.get(index-1));
        } else {
            rangeList.add(partitionSingleList.get(0));
            rangeList.add(partitionSingleList.get(partitionSingleList.size()-1));
        }
        return rangeList;
    }

    public boolean rewritePredicate(CompoundPredicate predicate, List<PartitionSingle> rangeList) {
        if (predicate.getOp() != CompoundPredicate.Operator.AND) {
            LOG.debug("predicate op {}", predicate.getOp().toString());
            return false;
        }
        for (Expr expr : predicate.getChildren()) {
            if (expr instanceof BinaryPredicate) {
                BinaryPredicate binPredicate = (BinaryPredicate) expr;
                BinaryPredicate.Operator op = binPredicate.getOp();
                if (binPredicate.getChildren().size() != 2) {
                    LOG.info("binary predicate children size {}", binPredicate.getChildren().size());
                    continue;
                }
                if (op == BinaryPredicate.Operator.NE) {
                    LOG.info("binary predicate op {}", op.toString());
                    continue;
                }
                PartitionKeyType key = new PartitionKeyType();
                switch (op) {
                    case LE: //<=
                        key.clone(rangeList.get(1).getPartitionKey());
                        break;
                    case LT: //<
                        key.clone(rangeList.get(1).getPartitionKey());
                        key.add(1);
                        break;
                    case GE: //>=
                        key.clone(rangeList.get(0).getPartitionKey());
                        break;
                    case GT: //>
                        key.clone(rangeList.get(0).getPartitionKey());
                        key.add(-1);
                        break;
                    default:
                        break;
                }
                LOG.info("op {}, val {}", op.toString(), key.realValue());
                LiteralExpr newLiteral;
                if( key.keyType == KeyType.DATE) {
                    try {
                        newLiteral = new DateLiteral(key.toString(), Type.DATE);
                    } catch (Exception e) {
                        LOG.warn("Date's format is error {},{}", key.toString(),e);
                        continue;
                    }
                } else if( key.keyType == KeyType.LONG){
                    newLiteral = new IntLiteral(key.realValue());
                }else{
                    LOG.warn("Partition cache not support type {}.", key.keyType);
                    continue;
                }

                if (binPredicate.getChild(1) instanceof LiteralExpr) {
                    binPredicate.removeNode(1);
                    binPredicate.addChild(newLiteral);
                } else if (binPredicate.getChild(0) instanceof LiteralExpr) {
                    binPredicate.removeNode(0);
                    binPredicate.setChild(0, newLiteral);
                } else {
                    continue;
                }
            } else if (expr instanceof InPredicate) {
                InPredicate inPredicate = (InPredicate) expr;
                if (!inPredicate.isLiteralChildren() || inPredicate.isNotIn()) {
                    continue;
                }
            }
        }
        return true;
    }

    /**
     * Get partition info from SQL Predicate and OlapTable
     * Pair<PartitionID, PartitionKey>
     * PARTITION BY RANGE(`olap_date`)
     * ( PARTITION p20200101 VALUES [("20200101"), ("20200102")),
     * PARTITION p20200102 VALUES [("20200102"), ("20200103")) )
     */
    private void getTablePartitionList(OlapTable table) {
        Map<Long, Range<PartitionKey>>  range =  rangePartitionInfo.getIdToRange();
        for(Map.Entry<Long, Range<PartitionKey>> entry : rangePartitionInfo.getIdToRange().entrySet() ) {
            Long partId = entry.getKey();
            for(PartitionSingle single : partitionSingleList) {
                if (entry.getValue().contains(single.getKey())) {
                    if( single.getPartitionId() == 0) {
                        single.setPartitionId(partId);
                    }
                }
            }
        }
        
        for(PartitionSingle single : partitionSingleList) {
            single.setPartition(table.getPartition(single.getPartitionId()));
        }
    }

    /**
     * Get value range of partition column from predicate
     */
    private void getPartitionKeyRange(PartitionColumnFilter partitionColumnFilter,
        Column partitionColumn) throws AnalysisException {
        if (partitionColumnFilter.lowerBound == null || partitionColumnFilter.upperBound == null) {
            LOG.info("filter is null");
            return;
        }
        PartitionKeyType begin = new PartitionKeyType();
        PartitionKeyType end = new PartitionKeyType();
        begin.init(partitionColumn.getType(), partitionColumnFilter.lowerBound);
        end.init(partitionColumn.getType(), partitionColumnFilter.upperBound);

        if (!partitionColumnFilter.lowerBoundInclusive) {
            begin.add(1);
        }
        if (!partitionColumnFilter.upperBoundInclusive) {
            end.add(-1);
        }
        if (begin.realValue() > end.realValue()) {
            LOG.info("partition range begin:{}, end{}", begin, end);
            return;
        }
    
        LOG.info("partition range begin:{}, end:{}", begin.toString(), end.toString());
        while (begin.realValue() <= end.realValue()) {
            PartitionKey key = PartitionKey.createPartitionKey(
                    Lists.newArrayList(new PartitionValue(begin.toString())),
                    Lists.newArrayList(partitionColumn));
            PartitionSingle single = new PartitionSingle();
            single.setPartitionKey(begin);
            single.setKey(key);
            partitionSingleList.add(single);
            begin.add(1);
        }
    }

    private PartitionColumnFilter createPartitionFilter(CompoundPredicate partitionKeyPredicate,
                                                        Column partitionColumn) {
        if( partitionKeyPredicate.getOp() != CompoundPredicate.Operator.AND ){
            LOG.debug("not and op");
            return null;
        }
        PartitionColumnFilter partitionColumnFilter = new PartitionColumnFilter();;
        for (Expr expr : partitionKeyPredicate.getChildren()) {
            if (expr instanceof BinaryPredicate) {
                BinaryPredicate binPredicate = (BinaryPredicate) expr;
                BinaryPredicate.Operator op = binPredicate.getOp();
                if( binPredicate.getChildren().size() != 2){
                    LOG.warn("child size {}", binPredicate.getChildren().size());
                    continue;
                }
                if (binPredicate.getOp() == BinaryPredicate.Operator.NE){
                    LOG.debug("not support NE operator");
                    continue;
                }
                Expr slotBinding;
                if (binPredicate.getChild(1) instanceof LiteralExpr) {
                    slotBinding = binPredicate.getChild(1);
                } else if (binPredicate.getChild(0) instanceof LiteralExpr) {
                    slotBinding = binPredicate.getChild(0);
                } else {
                    LOG.debug("not find LiteralExpr");
                    continue;
                }
                
                LiteralExpr literal = (LiteralExpr) slotBinding;
                LOG.info("LiteralExpr:{}, {}", literal.getType().toSql(),literal.toSql());
                switch (op) {
                    case EQ: //=
                        partitionColumnFilter.setLowerBound(literal, true);
                        partitionColumnFilter.setUpperBound(literal, true);
                        break;
                    case LE: //<=
                        partitionColumnFilter.setUpperBound(literal, true);
                        break;
                    case LT: //<
                        partitionColumnFilter.setUpperBound(literal, false);
                        break;
                    case GE: //>=
                        partitionColumnFilter.setLowerBound(literal, true);
                        
                        break;
                    case GT: //>
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
                partitionColumnFilter.setInPredicate(inPredicate);
            }
        }
        return partitionColumnFilter;
    }
}
