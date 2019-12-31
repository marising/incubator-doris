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
import org.apache.doris.system.Backend;

import java.util.List;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class CacheProxy {
    public class UpdateCacheValue extends FetchCacheParam {
        private RowBatch rowBatch;
        public UpdateCacheValue(long partitionKey, long lastVersion, long lastVersionTime, RowBatch rowBatch) {
            super(partitionKey, lastVersion, lastVersionTime);
            this.rowBatch = rowBatch;
        }
        public void getRpcValue(PUpdateCacheRequest request){
            PUpdateCacheValue value = request.new_value();
            value.set_partition_key(this.partitionKey);
            value.set_last_version(this.lastVersion);
            value.set_last_version_time(this.lastVersionTime);
        }
    }
    public class UpdateCacheRequest{
        private String sqlStr;
        private PUniqueId sqlKey;
        private List<UpdateCacheValue> valueList;
        public UpdateCacheRequest(String sqlStr){
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
        public void addParam(long partitionKey, long lastVersion, long lastVersionTime, RowBatch rowBatch){
            UpdateCacheValue value = new UpdateCacheValue(partitionKey, lastVersion, lastVersionTime, rowBatch);
            valueList.add(value);
        }

        public void getRpcRequest(PUpdateCacheRequest rpcReq){
            rpcReq.set_sql_key(sqlKey);
            for(UpdateCacheValue value : valueList){
                value.getRpcValue(rpcReq);
            }
        }
    }

    public class FetchCacheParam{
        protected long partitionKey;
        protected long lastVersion;
        protected long lastVersionTime;
        public FetchCacheParam(long partitionKey, long lastVersion, long lastVersionTime){
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
    }
    public class FetchCacheRequest{
        private String sqlStr;
        private PUniqueId sqlKey;
        private List<FetchCacheParam> paramList;
        public FetchCacheRequest(String sqlStr){
            this.sqlStr = sqlStr;
        }
        public String getSqlStr(){
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
        public void addParam(long partitionKey, long lastVersion, long lastVersionTime){
            FetchCacheParam param = new FetchCacheParam(partitionKey, lastVersion, lastVersionTime);
            paramList.add(param);
        }
    }

    public class CacheValue {
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
    }

    private static final Logger LOG = LogManager.getLogger(CacheProxy.class);

    public void updateCache(UpdateCacheRequest request) {
        PUniqueId sqlKey = getMd5(request.getSqlStr());
        request.setSqlKey(sqlKey);
        Backend backend = CachePartition.getInstance().findBackend(sqlKey);
        if (backend == null) {
            return;
        }
        TNetworkAddress address = TNetworkAddress(backend.getHost(), backend.getBePort());
        try{
            PUpdateCacheRequest rpcReq = new PUpdateCacheRequest();
            request.getRpcRequest(rpcReq);
            Future<PUpdateCacheResult> future = BackendServiceProxy.getInstance().updateCache(address, rpcReq);
        }catch (RpcException e) {
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
    }

    public List<CacheValue> fetchCache(FetchCacheRequest request) {
        PUniqueId sqlKey = getMd5(request.getSqlStr());
        request.setSqlKey(sqlKey);
        Backend backend = CachePartition.getInstance().findBackend(sqlKey);
        TNetworkAddress address = TNetworkAddress(backend.getHost(), backend.getBePort());
        final RowBatch rowBatch = new RowBatch();
        try {
            PFetchCacheRequest rpcReq = new PFetchCacheRequest();
            Future<PFetchDataResult> future = BackendServiceProxy.getInstance().fetchCache(address, request);
            PFetchDataResult pResult = null;
            while (pResult == null) {
                long currentTs = System.currentTimeMillis();
                if (currentTs >= timeoutTs) {
                    throw new TimeoutException("query timeout");
                }
                try {
                    pResult = future.get(timeoutTs - currentTs, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    LOG.info("future get interrupted Exception");
                }
            }
            TStatusCode code = TStatusCode.findByValue(pResult.status.status_code);
            if (code != TStatusCode.OK) {
                status.setPstatus(pResult.status);
                return null;
            }
            rowBatch.setQueryStatistics(pResult.query_statistics);
            if (packetIdx != pResult.packet_seq) {
                LOG.warn("receive packet failed, expect={}, receive={}", packetIdx, pResult.packet_seq);
                status.setRpcStatus("receive error packet");
                return null;
            }

            byte[] serialResult = request.getSerializedResult();
            if (serialResult != null && serialResult.length > 0) {
                TResultBatch resultBatch = new TResultBatch();
                TDeserializer deserializer = new TDeserializer();
                deserializer.deserialize(resultBatch, serialResult);
                rowBatch.setBatch(resultBatch);
                rowBatch.setEos(pResult.eos);
                result.rowBatch = rowBatch;
                return result;
            }
        } catch (RpcException e) {
            LOG.warn("fetch result rpc exception, finstId={}", finstId, e);
            status.setRpcStatus(e.getMessage());
            SimpleScheduler.addToBlacklist(backendId);
        } catch (
                ExecutionException e) {
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

    private PUniqueId getMd5(String str){
        msgDigest.reset();
        final byte[] digest = msgDigest.digest(str.getBytes());
        PUniqueId key;
        key.lo = getLong(digest, 0);
        key.hi = getLong(digest, 8);
        return key;
    }


    private static final long getLong(final byte[] array, final int offset) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value = ((value << 8) | (array[offset+i] & 0xFF));
        }
        return value;
    }
}
