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

package org.apache.doris.common.proc;

import org.apache.doris.alter.SchemaChangeHandler;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.LimitElement;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.OrderByPair;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.List;

public class SchemaChangeProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId").add("TableName").add("CreateTime").add("FinishTime")
            .add("IndexName").add("IndexId").add("OriginIndexId").add("SchemaVersion")
            .add("TransactionId").add("State").add("Msg").add("Progress").add("Timeout")
            .build();

    private static final Logger LOG = LogManager.getLogger(SchemaChangeProcNode.class);

    private SchemaChangeHandler schemaChangeHandler;
    private Database db;

    public SchemaChangeProcNode(SchemaChangeHandler schemaChangeHandler, Database db) {
        this.schemaChangeHandler = schemaChangeHandler;
        this.db = db;
    }

    boolean filterResult(String columnName, Comparable element, HashMap<String, Expr> filter) throws AnalysisException {
        if (filter == null) {
            return true;
        }
        Expr subExpr = filter.get(columnName.toLowerCase());
        if (subExpr == null) {
            return true;
        }
        BinaryPredicate binaryPredicate = (BinaryPredicate) subExpr;
        if (subExpr.getChild(1) instanceof StringLiteral && binaryPredicate.getOp() == BinaryPredicate.Operator.EQ) {
            LOG.info("SubExpr value " + ((StringLiteral) subExpr.getChild(1)).getValue() + ", real value " + element);
            return ((StringLiteral) subExpr.getChild(1)).getValue().equals(element);
        }
        if (subExpr.getChild(1) instanceof DateLiteral) {
            Long leftVal = (new DateLiteral((String) element, Type.DATETIME)).getLongValue();
            Long rightVal = ((DateLiteral) subExpr.getChild(1)).getLongValue();
            LOG.info("Left value " + leftVal + ", right value " + rightVal);
            switch( binaryPredicate.getOp()) {
                case EQ:
                case EQ_FOR_NULL:
                    return leftVal == rightVal;
                case GE:
                    return leftVal >= rightVal;
                case GT:
                    return leftVal > rightVal;
                case LE:
                    return leftVal <= rightVal;
                case LT:
                    return leftVal < rightVal;
                case NE:
                    return leftVal != rightVal;
                default:
                    Preconditions.checkState(false, "No defined binary operator.");
            }
        }
        return true;
    }

    public ProcResult fetchResultByFilter(HashMap<String, Expr> filter, ArrayList<OrderByPair> orderByPairs,
                                          LimitElement limitElement) throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(schemaChangeHandler);

        List<List<Comparable>> schemaChangeJobInfos = schemaChangeHandler.getAlterJobInfosByDb(db);

        LOG.info("Begin fetch data.Filter size " + filter.size() );
        for(Entry<String, Expr> entry : filter.entrySet()){
            LOG.info("key " + entry.getKey() + ", value" + entry.getValue().getChild(1));
        }

        //where
        List<List<Comparable>> jobInfos = new ArrayList<List<Comparable>>();
        for (List<Comparable> infoStr : schemaChangeJobInfos) {
            if (infoStr.size() != TITLE_NAMES.size()) {
                LOG.warn("SchemaChangeJobInfos.size() " + schemaChangeJobInfos.size()
                    + " not equal TITLE_NAMES.size() " + TITLE_NAMES.size());
                continue;
            }
            List<Comparable> jobInfo = new ArrayList<Comparable>();
            boolean isNeed = true;
            for (int i = 0; i < infoStr.size(); i++) {
                Comparable element = infoStr.get(i);
                isNeed = filterResult(TITLE_NAMES.get(i), element, filter);
                LOG.info("column "+ TITLE_NAMES.get(i) +", value " + element + ",isNeed " + isNeed);
                if (!isNeed) {
                    break;
                }
                jobInfo.add(element.toString());
            }
            if (isNeed) {
                jobInfos.add(jobInfo);
            }
        }

        // order by
        if (orderByPairs != null) {
            ListComparator<List<Comparable>> comparator = null;
            OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
            comparator = new ListComparator<List<Comparable>>(orderByPairs.toArray(orderByPairArr));
            Collections.sort(jobInfos, comparator);
        }

        //limit
        if (limitElement != null && limitElement.hasLimit()) {
            int beginIndex = (int) limitElement.getOffset();
            int endIndex = (int) (beginIndex + limitElement.getLimit());
            if (endIndex > jobInfos.size()) {
                endIndex = jobInfos.size();
            }
            jobInfos = jobInfos.subList(beginIndex,endIndex);
        }

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        for (List<Comparable> jobInfo : jobInfos) {
            List<String> oneResult = new ArrayList<String>(jobInfos.size());
            for (Comparable column : jobInfo) {
                oneResult.add(column.toString());
            }
            result.addRow(oneResult);
        }
        return result;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(schemaChangeHandler);

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        List<List<Comparable>> schemaChangeJobInfos = schemaChangeHandler.getAlterJobInfosByDb(db);
        for (List<Comparable> infoStr : schemaChangeJobInfos) {
            List<String> oneInfo = new ArrayList<String>(TITLE_NAMES.size());
            for (Comparable element : infoStr) {
                oneInfo.add(element.toString());
            }
            result.addRow(oneInfo);
        }
        return result;
    }

    public static int analyzeColumn(String columnName) throws AnalysisException {
        for (String title : TITLE_NAMES) {
            if (title.equalsIgnoreCase(columnName)) {
                return TITLE_NAMES.indexOf(title);
            }
        }
        throw new AnalysisException("Title name[" + columnName + "] does not exist");
    }
}
