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

import org.apache.doris.qe.cache.CacheProxy;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.qe.SimpleScheduler;
import org.apache.doris.system.Backend;
import org.apache.doris.proto.PUniqueId;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Hashtable;
import java.util.SortedMap;
import java.util.TreeMap;


public class CachePartition {
    private static final Logger LOG = LogManager.getLogger(CachePartition.class);
    private static final int VIRTUAL_NODES = 10;
    private Hashtable<Long, Backend> realNodes = new Hashtable<>();
    private SortedMap<Long, Backend> virtualNodes = new TreeMap<>();
    private static CachePartition cachePartition;

    public static CachePartition getInstance() {
        if (cachePartition == null) {
            cachePartition = new CachePartition();
        }
        return cachePartition;
    }

    protected CachePartition() {
    }

    /**
     * Using the consistent hash and the hi part of sqlkey to get the backend node
     * @param sqlKey 128 bit's sql md5
     * @return Backend
     */
    public Backend findBackend(PUniqueId sqlKey) {
        checkBackend();
        SortedMap<Long, Backend> headMap = virtualNodes.headMap(sqlKey.hi);
        SortedMap<Long, Backend> tailMap = virtualNodes.tailMap(sqlKey.hi);
        int retryTimes = 0;
        while (true) {
            if (tailMap == null || tailMap.size() == 0) {
                tailMap = headMap;
                retryTimes += 1;
                LOG.info("invalid tail map, retry {}", retryTimes); 
            }
            Long key = tailMap.firstKey();
            Backend virtualNode = tailMap.get(key);
            if (SimpleScheduler.isAlive(virtualNode)) {
                LOG.info("backend {} alive, key = {}, retry {}", virtualNode.getId(), key, retryTimes); 
                return virtualNode;
            } else {
                LOG.info("backend {} not alive, key = {}, retry {}", virtualNode.getId(), key, retryTimes); 
            }
            tailMap = tailMap.tailMap(key + 1);
            retryTimes++;
            if (retryTimes >= 5) {
                LOG.warn("reach max retry times {}", retryTimes); 
                return null;
            }
        }
    }

    public void checkBackend() {
        ImmutableMap<Long, Backend> idToBackend = Catalog.getCurrentSystemInfo().getIdToBackend();
        for (Backend backend : idToBackend.values().asList()) {
            addBackend(backend);
        }
    }

    public void addBackend(Backend backend) {
        if(realNodes.contains(backend.getId())) {
            return;
        }
        //LOG.info("Add backend id {}", backend.getId());
        realNodes.put(backend.getId(), backend);
        for (int i = 0; i < VIRTUAL_NODES; i++) {
            String nodeName = String.valueOf(backend.getId()) + "::" + String.valueOf(i);
            //Long hashCode = new Long(nodeName.hashCode());
            PUniqueId nodeId = CacheProxy.getMd5(nodeName);
            virtualNodes.put(nodeId.hi, backend);
            LOG.info("Add backend id {}, virtual node name {} hashcode {}", backend.getId(), nodeName, nodeId.hi);
        }
    }

    public boolean exitsBackend(Long id) {
        return realNodes.contains(id);
    }
}
