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

import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.proto.PCacheStatus;
import org.apache.doris.proto.PCacheParam;
import org.apache.doris.proto.PCacheValue;
import org.apache.doris.proto.PCacheResponse;
import org.apache.doris.proto.PUpdateCacheRequest;
import org.apache.doris.proto.PFetchCacheRequest;
import org.apache.doris.proto.PFetchCacheResult;
import org.apache.doris.proto.PClearCacheRequest;
import org.apache.doris.qe.RowBatch;
import org.apache.doris.qe.SimpleScheduler;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.common.Status;
import org.apache.doris.proto.PUniqueId;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TResultBatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.List;

public class CacheProxy {
    private static final Logger LOG = LogManager.getLogger(CacheProxy.class);
   
    public static void DebugTResultBatch(TResultBatch batch,String tag) {
        int idx = 0;
        LOG.info("TAG:{}", tag);
        for (ByteBuffer row : batch.getRows()) {
            idx++;
            byte[] bytes = Arrays.copyOfRange(row.array(), row.position(), row.limit());
            LOG.info("idx:{}, pos:{}, remain:{}, limit:{}, capacity:{},str:{}", idx, row.position(), row.remaining(),
                    row.limit(), row.capacity(), bytes.toString());
        }
    }

    public static class CacheParam extends PCacheParam {
        public CacheParam(PCacheParam param) {
            partition_key = param.partition_key;
            last_version = param.last_version;
            last_version_time = param.last_version_time;
        }
        public CacheParam(long partitionKey, long lastVersion, long lastVersionTime) {
            partition_key = partitionKey;
            last_version = lastVersion;
            last_version_time = lastVersionTime;
        }

        public PCacheParam getRParam() {
            PCacheParam param = new PCacheParam();
            param.partition_key = partition_key;
            param.last_version = last_version;
            param.last_version_time = last_version_time;
            return param;
        }

        public void Debug() {
            LOG.info("cache param, part key:{}, version:{}, time:{}",
                    partition_key, last_version, last_version_time);
        }
    }
 
    public static class CacheValue extends PCacheValue {
        public CacheParam param;
        public TResultBatch resultBatch;

        public CacheValue() {
            param = null;
            row = Lists.newArrayList();
            data_size = 0;
            resultBatch = new TResultBatch();
        }

        public void addRpcResult(PCacheValue value) {
            param = new CacheParam(value.param);
            //for (byte[] one : value.row) {
            //    resultBatch.addToRows(ByteBuffer.wrap(one));
            //}
            data_size += value.data_size;
            row.addAll(value.row);
        }

        public RowBatch getRowBatch() {
            for (byte[] one : row) {
                resultBatch.addToRows(ByteBuffer.wrap(one));
            }
            RowBatch batch = new RowBatch();
            resultBatch.setPacket_seq(1);
            resultBatch.setIs_compressed(false);
            batch.setBatch(resultBatch);
            batch.setEos(true);
            return batch;
        }

        public void addUpdateResult(long partitionKey, long lastVersion, long lastVersionTime, List<byte[]> rowList) {
            param = new CacheParam(partitionKey, lastVersion, lastVersionTime);
            for (byte[] buf : rowList) {
                data_size += buf.length;
                row.add(buf);
            }
        }

        public PCacheValue getRpcValue() {
            PCacheValue value = new PCacheValue();
            value.param = param.getRParam();
            value.data_size = data_size;
            value.row = row;
            return value;
        }

        public void Debug() {
            LOG.info("cache value, partkey:{}, ver:{}, time:{}, row_num:{}, data_size:{}",
                    param.partition_key, param.last_version, param.last_version_time,
                    row.size(),
                    data_size);
            for (int i = 0; i < row.size(); i++) {
                try {
                    String str = new String(row.get(i), "UTF-8");
                    LOG.info("{}:{}", i, str);
                }catch (Exception e){
                }
                LOG.info("{}:{}", i, row.get(i));
            }
        }
    }

    public static class UpdateCacheRequest extends PUpdateCacheRequest {
        private String sqlStr;
        private List<CacheValue> valueList;

        public UpdateCacheRequest(String sqlStr) {
            this.sqlStr = sqlStr;
            this.sql_key = getMd5(this.sqlStr);
            this.valueList = Lists.newArrayList();
        }

        public void addValue(long partitionKey, long lastVersion, long lastVersionTime, List<byte[]> rowList) {
            CacheValue value = new CacheValue();
            value.addUpdateResult(partitionKey, lastVersion, lastVersionTime, rowList);
            valueList.add(value);
        }

        public PUpdateCacheRequest getRpcRequest() {
            PUpdateCacheRequest request = new PUpdateCacheRequest();
            request.value = Lists.newArrayList();
            request.sql_key = sql_key;
            for (CacheValue value : valueList) {
                request.value.add(value.getRpcValue());
            }
            return request;
        }

        public void Debug() {
            LOG.info("update cache request, sql_key:{}, value_size:{}", DebugUtil.printId(sql_key),
                    valueList.size());
            for (CacheValue value : valueList) {
                value.Debug();
            }
        }
    }


    public static class FetchCacheRequest extends PFetchCacheRequest {
        private String sqlStr;
        private List<CacheParam> paramList;

        public FetchCacheRequest(String sqlStr) {
            this.sqlStr = sqlStr;
            this.sql_key = getMd5(this.sqlStr);
            this.paramList = Lists.newArrayList();
        }

        public void addParam(long partitionKey, long lastVersion, long lastVersionTime) {
            CacheParam param = new CacheParam(partitionKey, lastVersion, lastVersionTime);
            paramList.add(param);
        }

        public PFetchCacheRequest getRpcRequest() {
            PFetchCacheRequest request = new PFetchCacheRequest();
            request.param = Lists.newArrayList();
            request.sql_key = sql_key;
            for (CacheParam param : paramList) {
                request.param.add(param.getRParam());
            }
            return request;
        }

        public void Debug() {
            LOG.info("fetch cache request, sql_key:{}, param count:{}", DebugUtil.printId(sql_key), paramList.size());
            for (CacheParam param : paramList) {
                param.Debug();
            }
        }
    }

    public static class FetchCacheResult extends PFetchCacheResult {
        Status status;
        private List<CacheValue> valueList;

        public FetchCacheResult() {
            status = new Status();
            valueList = Lists.newArrayList();
        }

        public List<CacheValue> getValueList(){
            return valueList;
        }

        public void setResult(PFetchCacheResult rpcResult) {
            for (int i = 0; i < rpcResult.value.size(); i++) {
                PCacheValue rpcValue = rpcResult.value.get(i);
                CacheValue value = new CacheValue();
                value.addRpcResult(rpcValue);
                valueList.add(value);
                LOG.info("fetch cache, row:{}, size:{}", i, rpcValue.data_size);
            }
        }

        public void Debug() {
            LOG.info("fetch cache result, value size:{}", valueList.size());
            for (CacheValue value : valueList) {
                value.Debug();
            }
        }
    }

    public void updateCache(UpdateCacheRequest request, Status status) {
        PUniqueId sqlKey = request.sql_key;
        Backend backend = CachePartition.getInstance().findBackend(sqlKey);
        if (backend == null) {
            LOG.warn("update cache can't find backend, sqlKey:{}", sqlKey);
            return;
        }
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        try {
            PUpdateCacheRequest updateRequest = request.getRpcRequest();
            request.Debug();
            Future<PCacheResponse> future = BackendServiceProxy.getInstance().updateCache(address, updateRequest);
        } catch (RpcException e) {
            LOG.warn("update cache rpc exception, sqlKey:{}", sqlKey, e);
            status.setRpcStatus(e.getMessage());
            SimpleScheduler.addToBlacklist(backend.getId());
        } finally {
            //do nothing
        }
    }

    public FetchCacheResult fetchCache(FetchCacheRequest request, int timeoutMs, Status status) {
        PUniqueId sqlKey = request.sql_key;
        Backend backend = CachePartition.getInstance().findBackend(sqlKey);
        if (backend == null) {
            return null;
        }
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        long timeoutTs = System.currentTimeMillis() + timeoutMs;
        FetchCacheResult result = null;
        try {
            PFetchCacheRequest fetchRequest = request.getRpcRequest();
            request.Debug();
            Future<PFetchCacheResult> future = BackendServiceProxy.getInstance().fetchCache(address, fetchRequest);
            PFetchCacheResult fetchResult = null;
            while (fetchResult == null) {
                long currentTs = System.currentTimeMillis();
                if (currentTs >= timeoutTs) {
                    throw new TimeoutException("query cache timeout");
                }
                fetchResult = future.get(timeoutTs - currentTs, TimeUnit.MILLISECONDS);
                if (fetchResult.status != PCacheStatus.CACHE_OK) {
                    LOG.info("fetch catch null, status:{}", fetchResult.status);
                    return null;
                }
                result = new FetchCacheResult();
                result.setResult(fetchResult);
                result.Debug();
                return result;
            }
        } catch (RpcException e) {
            LOG.warn("fetch catch rpc exception, sqlKey:{}, backend:{}", sqlKey, backend.getId(), e);
            status.setRpcStatus(e.getMessage());
            SimpleScheduler.addToBlacklist(backend.getId());
        } catch (InterruptedException e) {
            LOG.warn("future get interrupted exception, sqlKey:{}, backend:{}", sqlKey, backend.getId(), e);
            status.setStatus("interrupted exception");
        } catch (ExecutionException e) {
            LOG.warn("future get execution exception, sqlKey:{}, backend:{}", sqlKey, backend.getId(), e);
            status.setStatus("execution exception");
        } catch (TimeoutException e) {
            LOG.warn("fetch result timeout, sqlKey:{}, backend:{}", sqlKey, backend.getId(), e);
            status.setStatus("query timeout");
        } finally {
        }
        return result;
    }

    public void clearCache(PClearCacheRequest request, List<Backend> beList) {
        int retry;
        for (Backend backend : beList) {
            retry = 1;
            while (retry < 3 && !this.clearCache(request, backend)) {
                retry++;
            }
            if (retry >= 3) {
                LOG.warn("clear cache timeout, backend:{}", backend.getId());
                SimpleScheduler.addToBlacklist(backend.getId());
            }
        }
    }

    private boolean clearCache(PClearCacheRequest request, Backend backend) {
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        try {
            request.clear_type = 0;
            LOG.info("clear all backend cache, backendId:{}", backend.getId());
            Future<PCacheResponse> future = BackendServiceProxy.getInstance().clearCache(address, request);
            return true;
        } catch (RpcException e) {
            LOG.warn("clear cache rpc exception, backendId:{}", backend.getId(), e);
            SimpleScheduler.addToBlacklist(backend.getId());
        } finally {
        }
        return false;
    }

    public static PUniqueId getMd5(String str) {
        MessageDigest msgDigest;
        try {
            msgDigest = MessageDigest.getInstance("MD5");
        } catch (Exception e) {
            return null;
        }
        final byte[] digest = msgDigest.digest(str.getBytes());
        PUniqueId key = new PUniqueId();
        key.lo = getLong(digest, 0);
        key.hi = getLong(digest, 8);
        return key;
    }

    public static final long getLong(final byte[] array, final int offset) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value = ((value << 8) | (array[offset + i] & 0xFF));
        }
        return value;
    }
}
