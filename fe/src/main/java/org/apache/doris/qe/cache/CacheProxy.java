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

import org.apache.doris.common.Status;
import org.apache.doris.qe.RowBatch;
import org.apache.doris.qe.SimpleScheduler;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class CacheProxy {
    public class UpdateCacheValue extends FetchCacheParam {
        private RowBatch rowBatch;
        public UpdateCacheValue(long partitionKey, long lastVersion, long lastVersionTime, RowBatch rowBatch) {
            super(partitionKey, lastVersion, lastVersionTime);
            this.rowBatch = rowBatch;
        }
        public void getRpcValue(PUpdateCacheRequest rpcRequest){
            PUpdateCacheValue value = rpcRequest.new_value();
            value.set_partition_key(this.partitionKey);
            value.set_last_version(this.lastVersion);
            value.set_last_version_time(this.lastVersionTime);
        }
    }
    public class UpdateCacheRequest {
        private String sqlStr;
        private PUniqueId sqlKey;
        private List<UpdateCacheValue> valueList;

        public UpdateCacheRequest(String sqlStr) {
            this.sqlStr = sqlStr;
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
            rpcRequest.set_sql_key(sqlKey);
            for (UpdateCacheValue value : valueList) {
                value.getRpcValue(rpcRequest);
            }
        }
    }

    public class FetchCacheParam {
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

        public void getRpcPram(PFetchCacheRequest rpcRequest){
            PFetchCacheParam param = rpcRequest.new_param();
            param.set_partition_key(this.partitionKey);
            param.set_last_version(this.lastVersion);
            param.set_last_version_time(this.lastVersionTime);
        }
    }

    public class FetchCacheRequest {
        private String sqlStr;
        private PUniqueId sqlKey;
        private List<FetchCacheParam> paramList;

        public FetchCacheRequest(String sqlStr) {
            this.sqlStr = sqlStr;
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
            rpcReq.set_sql_key(sqlKey);
            for (FetchCacheParam param : paramList) {
                param.getRpcPram(rpcReq);
            }
        }
    }

    public class FetchCacheValue {
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
        public void serialize(byte[] buffer, boolean eos) {
            TResultBatch resultBatch = new TResultBatch();
            TDeserializer deserializer = new TDeserializer();
            deserializer.deserialize(resultBatch, serialResult);
            rowBatch.setBatch(resultBatch);
            rowBatch.setEos(eos);
        }
    }

    public class FetchCacheResult{
        private List<FetchCacheValue> valueList;

        public List<FetchCacheValue> getValueList() {
            return valueList;
        }
        public void setValueList(List<FetchCacheValue> valueList) {
            this.valueList = valueList;
        }

        public void setResult(PFetchCacheResult rpcResult){
            for(int i = 0; rpcResult.value_size(); i++){
                PFetchCacheValue rpcValue = rpcResult.value(i);
                FetchCacheValue value = new FetchCacheValue();
                value.setPartitionKey(rpcValue.parition_key());
                value.serialize(rpcValue.row_batch().tuple_data());
                valueList.add(value);
            }
        }
    }

    private static final Logger LOG = LogManager.getLogger(CacheProxy.class);

    public void updateCache(UpdateCacheRequest request) {
        PUniqueId sqlKey = getMd5(request.getSqlStr());
        request.setSqlKey(sqlKey);
        Backend backend = CachePartition.getInstance().findBackend(sqlKey);
        if (backend == null) {
            LOG.warn("update cache cant find backend, sqlKey={}", sqlKey);
            return;
        }
        TNetworkAddress address = TNetworkAddress(backend.getHost(), backend.getBePort());
        try{
            PUpdateCacheRequest rpcReq = new PUpdateCacheRequest();
            request.getRpcRequest(rpcReq);
            Future<PUpdateCacheResult> future = BackendServiceProxy.getInstance().updateCache(address, rpcReq);
        }catch (RpcException e) {
            LOG.warn("update cache rpc exception, sqlKey={}", sqlKey, e);
            status.setRpcStatus(e.getMessage());
            SimpleScheduler.addToBlacklist(backendId);
        } catch (ExecutionException e) {
            LOG.warn("update cache execution exception, sqlKey={}", sqlKey, e);
            if (e.getMessage().contains("time out")) {
                // if timeout, we set error code to TIMEOUT, and it will not retry querying.
                status.setStatus(new Status(TStatusCode.TIMEOUT, e.getMessage()));
            } else {
                status.setRpcStatus(e.getMessage());
                SimpleScheduler.addToBlacklist(backend.getId());
            }
        } catch (TimeoutException e) {
            LOG.warn("update cache timeout, sqlKey={}", sqlKey, e);
            status.setStatus("query timeout");
        } finally {
            synchronized (this) {
                currentThread = null;
            }
        }
    }

    public FetchCacheResult fetchCache(FetchCacheRequest request,int timeoutMs) {
        PUniqueId sqlKey = getMd5(request.getSqlStr());
        request.setSqlKey(sqlKey);
        Backend backend = CachePartition.getInstance().findBackend(sqlKey);
        TNetworkAddress address = TNetworkAddress(backend.getHost(), backend.getBePort());
        long timeoutTs = System.currentTimeMillis() + timeoutMs;
        FetchCacheResult result = new FetchCacheResult();
        try {
            PFetchCacheRequest rpcRequest = new PFetchCacheRequest();
            request.getRpcRequest(rpcRequest);
            Future<PFetchDataResult> future = BackendServiceProxy.getInstance().fetchCache(address, rpcRequest);
            PFetchDataResult rpcResult = null;
            while (rpcResult == null) {
                long currentTs = System.currentTimeMillis();
                if (currentTs >= timeoutTs) {
                    throw new TimeoutException("query cache timeout");
                }
                try {
                    rpcResult = future.get(timeoutTs - currentTs, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    LOG.info("future get interrupted Exception");
                }
                PCacheStatus code = PCacheStatus.findByValue(rpcResult.status.status_code);
                code = PCacheStatus.findByValue(rpcResult.status.status_code);
                if (code != PStatusCode.FETCH_SUCCESS) {
                    return null;
                }
                result.setResult(rpcResult);
                return result;
            }
        } catch (RpcException e) {
            LOG.warn("fetch result rpc exception, finstId={}", finstId, e);
            status.setRpcStatus(e.getMessage());
            SimpleScheduler.addToBlacklist(backendId);
        } catch (ExecutionException e) {
            LOG.warn("fetch result execution exception, finstId={}", finstId, e);
            if (e.getMessage().contains("time out")) {
                // if timeout, we set error code to TIMEOUT, and it will not retry querying.
                status.setStatus(new Status(TStatusCode.TIMEOUT, e.getMessage()));
            } else {
                status.setRpcStatus(e.getMessage());
                SimpleScheduler.addToBlacklist(backendId);
            }
        } catch (
                TimeoutException e) {
            LOG.warn("fetch result timeout, finstId={}", finstId, e);
            status.setStatus("query timeout");
        } finally {
            synchronized (this) {
                currentThread = null;
            }
        }
        result.rowBatch = rowBatch;
        return result;
    }

    public void clearCache(UpdateCacheRequest request, List<Backend> beList) {
        for(Backend backend : beList){
            this.clearCache(request, backend);
        }
    }

    private void clearCache(UpdateCacheRequest request, Backend backend){
        TNetworkAddress address = TNetworkAddress(backend.getHost(), backend.getBePort());
        try{
            PUpdateCacheRequest rpcReq = new PUpdateCacheRequest();
            Future<PUpdateCacheResult> future = BackendServiceProxy.getInstance().clearCache(address, rpcReq);
        }catch (RpcException e) {
            LOG.warn("update cache rpc exception, sqlKey={}", sqlKey, e);
            status.setRpcStatus(e.getMessage());
            SimpleScheduler.addToBlacklist(backendId);
        } catch (ExecutionException e) {
            LOG.warn("update cache execution exception, sqlKey={}", sqlKey, e);
            if (e.getMessage().contains("time out")) {
                // if timeout, we set error code to TIMEOUT, and it will not retry querying.
                status.setStatus(new Status(TStatusCode.TIMEOUT, e.getMessage()));
            } else {
                status.setRpcStatus(e.getMessage());
                SimpleScheduler.addToBlacklist(backend.getId());
            }
        } catch (TimeoutException e) {
            LOG.warn("update cache timeout, sqlKey={}", sqlKey, e);
            status.setStatus("query timeout");
        } finally {
            synchronized (this) {
                currentThread = null;
            }
        }
    }
}
