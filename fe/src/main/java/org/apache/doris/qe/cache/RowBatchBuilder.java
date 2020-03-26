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

import org.apache.doris.thrift.TResultBatch;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RowBatchBuilder {
    private static final Logger LOG = LogManager.getLogger(RowBatchBuilder.class);

    private CacheProxy.UpdateCacheRequest updateRequest;
    private CacheAnalyzer.CacheModel cacheModel;
    private int begin;
    private Type keyType;
    private HashMap<Long, PartitionRange.PartitionSingle> partMap;
    private List<RowBatch> rowBatchList;
    private int rowSize;

    public int getRowSize() { return rowSize; }

    public RowBatchBuilder(CacheAnalyzer.CacheModel model) {
        cacheModel = model;
        begin = 0;
        keyType = Type.INVALID;
        rowBatchList = Lists.newArrayList();
        partMap = new HashMap<>();
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

    public void appendRowBatch(RowBatch rowBatch) {
        rowSize += rowBatch.getBatch().getRowsSize();
        rowBatchList.add(rowBatch);
    }

    public void buildSqlUpdateRequest(String sql, long lastestTime) {
        if (updateRequest == null) {
            updateRequest = new CacheProxy.UpdateCacheRequest(sql);
        }
        int packet_num = 0;
        for (RowBatch batch : rowBatchList) {
            TResultBatch result = new TResultBatch();
            result.setRows(batch.getBatch().getRows());
            result.setPacket_seq(packet_num);
            result.setIs_compressed(false);
            updateRequest.addValue(0, 0, lastestTime, result);
        }
        LOG.info("build update request, sql_key:{}, batch size:{}, packet num:{}",
                DebugUtil.printId(updateRequest.getSqlKey()),
                rowBatchList.size(), packet_num);
    }

    /**
     * Rowbatch split to TResultData
     */
    public void buildPartitionUpdateRequest(String sql) {
        if (updateRequest == null) {
            updateRequest = new CacheProxy.UpdateCacheRequest(sql);
        }
        HashMap<Long, List<ByteBuffer>> partRowMap = new HashMap<>();
        long partKey = 0;
        for (RowBatch batch : rowBatchList) {
            for (ByteBuffer row : batch.getBatch().getRows()) {
                if (Type.SMALLINT.equals(keyType)) {
                    partKey = row.getShort(begin);
                } else if (Type.INT.equals(keyType)) {
                    partKey = row.getInt(begin);
                } else if (Type.BIGINT.equals(keyType)) {
                    partKey = row.getLong(begin);
                }
                List<ByteBuffer> rowBufferList;
                if (!partRowMap.containsKey(partKey)) {
                    rowBufferList = Lists.newArrayList();
                    rowBufferList.add(row);
                    partRowMap.put(partKey, rowBufferList);
                } else {
                    partRowMap.get(partKey).add(row);
                }
            }
        }
        int packet_num = 0;
        for (HashMap.Entry<Long, List<ByteBuffer>> entry : partRowMap.entrySet()) {
            Long key = entry.getKey();
            PartitionRange.PartitionSingle partition = partMap.get(key);
            if (partition == null) {
                LOG.warn("cant find partition");
                continue;
            }
            List<ByteBuffer> buffer = entry.getValue();
            TResultBatch result = new TResultBatch();
            result.setRows(buffer);
            result.setPacket_seq(packet_num);
            result.setIs_compressed(false);
            updateRequest.addValue(key, partition.getPartition().getVisibleVersion(),
                    partition.getPartition().getVisibleVersionTime(), result);
            packet_num++;
        }
        LOG.info("build update request, sql_key:{}, batch size:{}, packet num:{}",
                DebugUtil.printId(updateRequest.getSqlKey()),
                rowBatchList.size(), packet_num);
    }

    public CacheProxy.UpdateCacheRequest getUpdateRequest() {
        return updateRequest;
    }

    public void setUpdateRequest(CacheProxy.UpdateCacheRequest updateRequest) {
        this.updateRequest = updateRequest;
    }
}
