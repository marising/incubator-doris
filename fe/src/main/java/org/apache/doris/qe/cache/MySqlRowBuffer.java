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

import org.apache.doris.analysis.Expr;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.qe.RowBatch;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class MySqlRowBuffer {
    private ArrayList<Expr> resultExprList;
    private List<String> columnLabelList;
    private Column keyColumn;
    private CacheProxy.UpdateCacheRequest updateRequest;
    int begin;
    Type keyType;
    public MySqlRowBuffer(ArrayList<Expr> resultExpr, List<String> columnLabel, Column key) {
        resultExprList = resultExpr;
        columnLabelList = columnLabel;
        keyColumn = key;
        keyType = Type.INVALID;
        this.init();
    }

    public void init() {
        for (int i = 0; i < columnLabelList.size(); i++) {
            int size = resultExprList.get(i).getType().getColumnSize();
            if (columnLabelList.get(i).equalsIgnoreCase(keyColumn.getName())) {
                keyType = resultExprList.get(i).getType();
                break;
            }
            begin += size;
        }
    }

    public void appendRowBatch(RowBatch rowBatch) {
        if (keyType == Type.INVALID) {
            return;
        }
        long partKey = 0;
        for (ByteBuffer row : rowBatch.getBatch().getRows()) {
            if (Type.SMALLINT.equals(keyType)) {
                partKey = row.getShort(begin);
            } else if (Type.INT.equals(keyType)) {
                partKey = row.getInt(begin);
            } else if (Type.BIGINT.equals(keyType)) {
                partKey = row.getLong(begin);
            }
        }
        getUpdateRequest().addValue(partKey, 0, 0, rowBatch);
    }

    public CacheProxy.UpdateCacheRequest getUpdateRequest() {
        return updateRequest;
    }

    public void setUpdateRequest(CacheProxy.UpdateCacheRequest updateRequest) {
        this.updateRequest = updateRequest;
    }
}
