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

import org.apache.doris.proto.PCacheStatus;
import org.apache.doris.proto.PCacheResponse;
import org.apache.doris.proto.PUpdateCacheRequest;
import org.apache.doris.proto.PFetchCacheRequest;
import org.apache.doris.proto.PFetchCacheResult;
import org.apache.doris.proto.PClearCacheRequest;
import org.apache.doris.qe.SimpleScheduler;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.common.Status;
import org.apache.doris.proto.PUniqueId;
import org.apache.doris.thrift.TNetworkAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.List;

public class CacheBeProxy extends CacheProxy {
    private static final Logger LOG = LogManager.getLogger(CacheBeProxy.class);

    public void updateCache(UpdateCacheRequest request, Status status) {
        PUniqueId sqlKey = request.sql_key;
        Backend backend = CacheCoordinator.getInstance().findBackend(sqlKey);
        if (backend == null) {
            LOG.warn("update cache can't find backend, sqlKey {}", sqlKey);
            return;
        }
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        try {
            PUpdateCacheRequest updateRequest = request.getRpcRequest();
            request.Debug();
            Future<PCacheResponse> future = BackendServiceProxy.getInstance().updateCache(address, updateRequest);
        } catch (RpcException e) {
            LOG.warn("update cache rpc exception, sqlKey {}, e {}", sqlKey, e);
            status.setRpcStatus(e.getMessage());
            SimpleScheduler.addToBlacklist(backend.getId());
        } finally {
            //do nothing
        }
    }

    public FetchCacheResult fetchCache(FetchCacheRequest request, int timeoutMs, Status status) {
        PUniqueId sqlKey = request.sql_key;
        Backend backend = CacheCoordinator.getInstance().findBackend(sqlKey);
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
                    LOG.info("fetch catch null, status {}", fetchResult.status);
                    return null;
                }
                result = new FetchCacheResult();
                result.setResult(fetchResult);
                result.Debug();
                status.setStatus(TStatusCode.OK);
                return result;
            }
        } catch (RpcException e) {
            LOG.warn("fetch catch rpc exception, sqlKey {}, backend {}", sqlKey, backend.getId(), e);
            status.setRpcStatus(e.getMessage());
            SimpleScheduler.addToBlacklist(backend.getId());
        } catch (InterruptedException e) {
            LOG.warn("future get interrupted exception, sqlKey {}, backend {}", sqlKey, backend.getId(), e);
            status.setStatus("interrupted exception");
        } catch (ExecutionException e) {
            LOG.warn("future get execution exception, sqlKey {}, backend {}", sqlKey, backend.getId(), e);
            status.setStatus("execution exception");
        } catch (TimeoutException e) {
            LOG.warn("fetch result timeout, sqlKey {}, backend {}", sqlKey, backend.getId(), e);
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
                LOG.warn("clear cache timeout, backend {}", backend.getId());
                SimpleScheduler.addToBlacklist(backend.getId());
            }
        }
    }

    protected boolean clearCache(PClearCacheRequest request, Backend backend) {
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        try {
            request.clear_type = 0;
            LOG.info("clear all backend cache, backendId {}", backend.getId());
            Future<PCacheResponse> future = BackendServiceProxy.getInstance().clearCache(address, request);
            return true;
        } catch (RpcException e) {
            LOG.warn("clear cache rpc exception, backendId {}", backend.getId(), e);
            SimpleScheduler.addToBlacklist(backend.getId());
        } finally {
        }
        return false;
    }
}
