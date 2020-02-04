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

import org.apache.doris.qe.RowBatch;
import org.apache.doris.qe.SimpleScheduler;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.common.Status;

import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TResultBatch;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

import org.apache.doris.proto.PUpdateCacheRequest;
import org.apache.doris.proto.PUpdateCacheResult;
import org.apache.doris.proto.PUpdateCacheValue;
import org.apache.doris.proto.PFetchCacheRequest;
import org.apache.doris.proto.PFetchCacheParam;
import org.apache.doris.proto.PFetchCacheResult;
import org.apache.doris.proto.PFetchCacheValue;
import org.apache.doris.proto.PCacheStatus;
import org.apache.doris.proto.PUniqueId;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.MessageDigest;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.List;

public class CacheProxy {
    private static final Logger LOG = LogManager.getLogger(CacheProxy.class);

    public static class UpdateCacheValue extends FetchCacheParam {
        private RowBatch rowBatch;
        public UpdateCacheValue(long partitionKey, long lastVersion, long lastVersionTime, RowBatch rowBatch) {
            super(partitionKey, lastVersion, lastVersionTime);
            this.rowBatch = rowBatch;
        }
        public void getRpcValue(PUpdateCacheRequest rpcRequest) {
            PUpdateCacheValue value = new PUpdateCacheValue();
            value.partition_key = this.partitionKey;
            value.last_version = this.lastVersion;
            value.last_version_time = this.lastVersionTime;
            rpcRequest.value.add(value);
        }
    }

    public static class UpdateCacheRequest {
        private String sqlStr;
        private PUniqueId sqlKey;
        private List<UpdateCacheValue> valueList;

        public UpdateCacheRequest(String sqlStr) {
            this.sqlStr = sqlStr;
            this.sqlKey = getMd5(this.sqlStr);
        }

        public String getSqlStr() {
            return sqlStr;
        }

        public PUniqueId getSqlKey() {
            return sqlKey;
        }

        public void setSqlKey(PUniqueId sqlKey) {
            this.sqlKey = sqlKey;
        }

        public List<UpdateCacheValue> getValueList() {
            return valueList;
        }

        public void addValue(long partitionKey, long lastVersion, long lastVersionTime, RowBatch rowBatch) {
            UpdateCacheValue value = new UpdateCacheValue(partitionKey, lastVersion, lastVersionTime, rowBatch);
            valueList.add(value);
        }

        public void getRpcRequest(PUpdateCacheRequest rpcRequest) {
            rpcRequest.sql_key = sqlKey;
            for (UpdateCacheValue value : valueList) {
                value.getRpcValue(rpcRequest);
            }
        }
    }

    public static class FetchCacheParam {
        protected long partitionKey;
        protected long lastVersion;
        protected long lastVersionTime;

        public FetchCacheParam(long partitionKey, long lastVersion, long lastVersionTime) {
            this.partitionKey = partitionKey;
            this.lastVersion = lastVersion;
            this.lastVersionTime = lastVersionTime;
        }

        public long getPartitionKey() {
            return partitionKey;
        }
        public long getLastVersion() {
            return lastVersion;
        }
        public long getLastVersionTime() {
            return lastVersionTime;
        }

        public void getRpcPram(PFetchCacheRequest rpcRequest) {
            PFetchCacheParam param = new PFetchCacheParam();
            param.partition_key = this.partitionKey;
            param.last_version = this.lastVersion;
            param.last_version_time = this.lastVersionTime;
            rpcRequest.param.add(param);
        }
    }

    public static class FetchCacheRequest {
        private String sqlStr;
        private PUniqueId sqlKey;
        private List<FetchCacheParam> paramList;

        public FetchCacheRequest(String sqlStr) {
            this.sqlStr = sqlStr;
            this.sqlKey = getMd5(this.sqlStr);
        }

        public String getSqlStr() {
            return sqlStr;
        }

        public PUniqueId getSqlKey() {
            return sqlKey;
        }

        public void setSqlKey(PUniqueId sqlKey) {
            this.sqlKey = sqlKey;
        }

        public List<FetchCacheParam> getParamList() {
            return paramList;
        }

        public void addParam(long partitionKey, long lastVersion, long lastVersionTime) {
            FetchCacheParam param = new FetchCacheParam(partitionKey, lastVersion, lastVersionTime);
            paramList.add(param);
        }

        public void getRpcRequest(PFetchCacheRequest rpcReq) {
            rpcReq.sql_key = sqlKey;
            for (FetchCacheParam param : paramList) {
                param.getRpcPram(rpcReq);
            }
        }
    }

    public static class FetchCacheValue {
        private long partitionKey;
        private RowBatch rowBatch;

        public long getPartitionKey() {
            return partitionKey;
        }

        public void setPartitionKey(long partitionKey) {
            this.partitionKey = partitionKey;
        }
        public RowBatch getRowBatch() {
            return rowBatch;
        }
        public void setRowBatch(RowBatch rowBatch) {
            this.rowBatch = rowBatch;
        }
        public void serialize(byte[] buffer, boolean eos) throws TException {
            TResultBatch resultBatch = new TResultBatch();
            TDeserializer deserializer = new TDeserializer();
            deserializer.deserialize(resultBatch, buffer);
            rowBatch.setBatch(resultBatch);
            rowBatch.setEos(eos);
        }
    }

    public static class FetchCacheResult {
        private List<FetchCacheValue> valueList;

        public List<FetchCacheValue> getValueList() {
            return valueList;
        }
        public void setValueList(List<FetchCacheValue> valueList) {
            this.valueList = valueList;
        }

        public void setResult(PFetchCacheResult rpcResult) throws TException {
            for (int i = 0; i < rpcResult.value.size(); i++) {
                PFetchCacheValue rpcValue = rpcResult.value.get(i);
                FetchCacheValue value = new FetchCacheValue();
                value.setPartitionKey(rpcValue.partition_key);
                if (i == rpcResult.value.size()-1) {
                    value.serialize(rpcValue.row_batch.tuple_data,true);
                } else {
                    value.serialize(rpcValue.row_batch.tuple_data,false);
                }
                valueList.add(value);
            }
        }
    }

    public void updateCache(UpdateCacheRequest request, Status status) {
        PUniqueId sqlKey = request.getSqlKey();
        Backend backend = CachePartition.getInstance().findBackend(sqlKey);
        if (backend == null) {
            LOG.warn("update cache can't find backend, sqlKey={}", sqlKey);
            return;
        }
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBePort());
        try{
            PUpdateCacheRequest rpcReq = new PUpdateCacheRequest();
            request.getRpcRequest(rpcReq);
            Future<PUpdateCacheResult> future = BackendServiceProxy.getInstance().updateCache(address, rpcReq);
        }catch (RpcException e) {
            LOG.warn("update cache rpc exception, sqlKey={}", sqlKey, e);
            status.setRpcStatus(e.getMessage());
            SimpleScheduler.addToBlacklist(backend.getId());
        } finally {
            //do nothing
        }
    }

    public FetchCacheResult fetchCache(FetchCacheRequest request,int timeoutMs, Status status) {
        PUniqueId sqlKey = request.getSqlKey();
        Backend backend = CachePartition.getInstance().findBackend(sqlKey);
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBePort());
        long timeoutTs = System.currentTimeMillis() + timeoutMs;
        FetchCacheResult result = new FetchCacheResult();
        try {
            PFetchCacheRequest rpcRequest = new PFetchCacheRequest();
            request.getRpcRequest(rpcRequest);
            Future<PFetchCacheResult> future = BackendServiceProxy.getInstance().fetchCache(address, rpcRequest);
            PFetchCacheResult rpcResult = null;
            while (rpcResult == null) {
                long currentTs = System.currentTimeMillis();
                if (currentTs >= timeoutTs) {
                    throw new TimeoutException("query cache timeout");
                }
                rpcResult = future.get(timeoutTs - currentTs, TimeUnit.MILLISECONDS);
                if (rpcResult.status != PCacheStatus.FETCH_SUCCESS) {
                    return null;
                }
                result.setResult(rpcResult);
                return result;
            }
        } catch (RpcException e) {
            LOG.warn("fetch catch rpc exception, sqlKey={}, backend={}", sqlKey, backend.getId(), e);
            status.setRpcStatus(e.getMessage());
            SimpleScheduler.addToBlacklist(backend.getId());
        } catch (InterruptedException e) {
            LOG.warn("future get interrupted Exception, sqlKey={}", sqlKey, e);
            status.setStatus("interrupted exception");
        } catch (ExecutionException e) {
            LOG.warn("future get execution exception, sqlKey={}", sqlKey, e);
            status.setStatus("execution exception");
        } catch (TException e) {
            LOG.warn("fetch result deserialize error, sqlKey={}", sqlKey, e);            
            status.setStatus("deserialize error");
        } catch (TimeoutException e) {
            LOG.warn("fetch result timeout, sqlKey={}", sqlKey, e);
            status.setStatus("query timeout");
        } finally {
        }
        return result;
    }

    public void clearCache(UpdateCacheRequest request, List<Backend> beList) {
        int retry;
        for (Backend backend : beList) {
            retry = 1;
            while (retry < 3 && !this.clearCache(request, backend)) {
                retry++;
            }
            if (retry >= 3) {
                LOG.warn("clear cache timeout, backend={}", backend.getId());
                SimpleScheduler.addToBlacklist(backend.getId());
            }
        }
    }

    private boolean clearCache(UpdateCacheRequest request, Backend backend) {
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBePort());
        try{
            PUpdateCacheRequest rpcReq = new PUpdateCacheRequest();
            LOG.info("clear all backend cache, backendId={}", backend.getId());
            Future<PUpdateCacheResult> future = BackendServiceProxy.getInstance().clearCache(address, rpcReq);
            return true;
        }catch (RpcException e) {
            LOG.warn("clear cache rpc exception, backendId={}", backend.getId(), e);
            SimpleScheduler.addToBlacklist(backend.getId());
        } finally {
        }
        return false; 
    }

    public static PUniqueId getMd5(String str) {
        MessageDigest msgDigest;
        try {
            msgDigest = MessageDigest.getInstance("MD5");
        } catch(Exception e) {
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
            value = ((value << 8) | (array[offset+i] & 0xFF));
        }
        return value;
    }
}
