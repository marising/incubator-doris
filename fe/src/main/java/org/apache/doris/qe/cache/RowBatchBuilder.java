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
import org.apache.doris.analysis.Expr;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.qe.RowBatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class RowBatchBuilder {
    private static final Logger LOG = LogManager.getLogger(RowBatchBuilder.class);

    private CacheProxy.UpdateCacheRequest updateRequest;
    private CacheAnalyzer.CacheModel cacheModel;
    private int begin;
    private Type keyType;
    private HashMap<Long, PartitionRange.PartitionSingle> partMap;
    private List<byte[]> rowList;
    private int batchSize;
    private int rowSize;
    private int dataSize;

    public int getRowSize() { return rowSize; }

    public RowBatchBuilder(CacheAnalyzer.CacheModel model) {
        cacheModel = model;
        begin = 0;
        keyType = Type.INVALID;
        rowList = Lists.newArrayList();
        partMap = new HashMap<>();
        batchSize = 0;
        rowSize = 0;
        dataSize = 0;
    }

    public void partitionIndex(ArrayList<Expr> resultExpr,
                               List<String> columnLabel, Column partColumn,
                               List<PartitionRange.PartitionSingle> partitionSingleList) {
        if (cacheModel != CacheAnalyzer.CacheModel.Partition) {
            return;
        }
        for (int i = 0; i < columnLabel.size(); i++) {
            int size = resultExpr.get(i).getType().getColumnSize();
            if (columnLabel.get(i).equalsIgnoreCase(partColumn.getName())) {
                keyType = resultExpr.get(i).getType();
                break;
            }
            begin += size;
        }
        for (PartitionRange.PartitionSingle single : partitionSingleList) {
            single.Debug();
            partMap.put(single.getPartitionKey(), single);
        }
        LOG.info("part name:{}, type:{}, result index:{}, range size:{} ", partColumn.getName(), keyType,
                begin, partMap.size());
    }

    public void copyRowData(RowBatch rowBatch) {
        batchSize ++;
        rowSize += rowBatch.getBatch().getRowsSize();
        for (ByteBuffer buf : rowBatch.getBatch().getRows()) {
            byte[] bytes = Arrays.copyOfRange(buf.array(), buf.position(), buf.limit());
            dataSize += bytes.length;
            rowList.add(bytes);
            LOG.info("row:{}", bytes.toString());
        }
    }

    public void buildSqlUpdateRequest(String sql, long lastestTime) {
        if (updateRequest == null) {
            updateRequest = new CacheProxy.UpdateCacheRequest(sql);
        }
        updateRequest.addValue(0, 0, lastestTime, rowList);
        LOG.info("build sql update request, sql_key:{}, batch:{}, row:{}, data:{}",
                DebugUtil.printId(updateRequest.sql_key),
                batchSize, rowSize, dataSize);
    }

    /**
     * Rowbatch split to TResultData
     */
    public void buildPartitionUpdateRequest(String sql) {
        if (updateRequest == null) {
            updateRequest = new CacheProxy.UpdateCacheRequest(sql);
        }
        HashMap<Long, List<byte[]>> partRowMap = new HashMap<>();
        List<byte[]> partitionRowList;
        long partKey = 0;
        for (byte[] buf : rowList) {
            ByteBuffer row = ByteBuffer.wrap(buf);
            if (Type.SMALLINT.equals(keyType)) {
                partKey = row.getShort(begin);
            } else if (Type.INT.equals(keyType)) {
                partKey = row.getInt(begin);
            } else if (Type.BIGINT.equals(keyType)) {
                partKey = row.getLong(begin);
            }
            if (!partRowMap.containsKey(partKey)) {
                partitionRowList = Lists.newArrayList();
                partitionRowList.add(buf);
                partRowMap.put(partKey, partitionRowList);
            } else {
                partRowMap.get(partKey).add(buf);
            }
        }

        for (HashMap.Entry<Long, List<byte[]>> entry : partRowMap.entrySet()) {
            Long key = entry.getKey();
            PartitionRange.PartitionSingle partition = partMap.get(key);
            if (partition == null) {
                LOG.warn("cant find partition");
                continue;
            }
            partitionRowList = entry.getValue();
            updateRequest.addValue(key, partition.getPartition().getVisibleVersion(),
                    partition.getPartition().getVisibleVersionTime(), partitionRowList);
            LOG.info("build partition update request, sql_key:{}, part_key:{}, row_size:{}",
                    DebugUtil.printId(updateRequest.sql_key),
                    key,
                    partitionRowList.size());
        }
    }

    public CacheProxy.UpdateCacheRequest getUpdateRequest() {
        return updateRequest;
    }

    public void setUpdateRequest(CacheProxy.UpdateCacheRequest updateRequest) {
        this.updateRequest = updateRequest;
    }
}
