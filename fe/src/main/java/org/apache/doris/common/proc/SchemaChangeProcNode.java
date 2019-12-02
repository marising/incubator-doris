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
import org.apache.doris.analysis.*;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.common.util.TimeUtils;

import javax.validation.constraints.Null;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class SchemaChangeProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId").add("TableName").add("CreateTime").add("FinishTime")
            .add("IndexName").add("IndexId").add("OriginIndexId").add("SchemaVersion")
            .add("TransactionId").add("State").add("Msg").add("Progress").add("Timeout")
            .build();

    private SchemaChangeHandler schemaChangeHandler;
    private Database db;

    public SchemaChangeProcNode(SchemaChangeHandler schemaChangeHandler, Database db) {
        this.schemaChangeHandler = schemaChangeHandler;
        this.db = db;
    }

    boolean isFiltered(String columnName, Comparable element, HashMap<String, Expr> filter){
        if (filter == null) {
            return false;
        }
        Expr subExpr = filter.get(columnName.toLowerCase());
        if (subExpr == null) {
            return false;
        }
        BinaryPredicate binaryPredicate = (BinaryPredicate) subExpr;
        if ( subExpr.getChild(1) instanceof StringLiteral && binaryPredicate.getOp() == BinaryPredicate.Operator.EQ) {
            return ((StringLiteral) subExpr.getChild(1)).getValue() == element;
        }
        if( subExpr.getChild(1) instanceof  DateLiteral ){
            Long leftVal = TimeUtils.timeStringToLong((String)element);
            Long rightVal = ((DateLiteral)subExpr.getChild(1)).getLongValue();
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
        return false;
    }

    public ProcResult fetchResultByFilter(HashMap<String, Expr> filter, ArrayList<OrderByPair> orderByPairs,
                                          LimitElement limitElement) throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(schemaChangeHandler);

        List<List<Comparable>> schemaChangeJobInfos = schemaChangeHandler.getAlterJobInfosByDb(db);
        if (schemaChangeJobInfos.size() != TITLE_NAMES.size()) {
            throw new AnalysisException("schemaChangeJobInfos.size() " + schemaChangeJobInfos.size()
                    + " not equal TITLE_NAMES.size() " + TITLE_NAMES.size());
        }

        //where
        List<List<Comparable>> jobInfos = new ArrayList<List<Comparable>>();
        for (List<Comparable> infoStr : schemaChangeJobInfos) {
            List<Comparable> jobInfo = new ArrayList<Comparable>();
            boolean isFilter = false;
            for (int i = 0; i < infoStr.size(); i++) {
                Comparable element = infoStr.get(i);
                isFilter = isFiltered(TITLE_NAMES.get(i), element, filter);
                if (isFilter) {
                    continue;
                }
                jobInfo.add(element.toString());
            }
            if (!isFilter) {
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
            jobInfos.subList((int) limitElement.getOffset(),
                    (int) (limitElement.getOffset() + limitElement.getLimit()));
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
