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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.qe.SimpleScheduler;
import org.apache.doris.system.Backend;
import org.apache.doris.proto.PUniqueId;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Hashtable;
import java.util.SortedMap;
import java.util.LinkedList;
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
        int retryCount = 0;
        while (true) {
            if (tailMap == null || tailMap.size() == 0) {
                tailMap = headMap;
                retryCount += 1;
            }
            Long key = tailMap.firstKey();
            Backend virtualNode = tailMap.get(key);
            if (SimpleScheduler.isAlive(virtualNode)) {
                return virtualNode;
            }
            tailMap = tailMap.tailMap(key + 1);
            retryCount += 1;
            if (retryCount >= 5) {
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
        realNodes.put(backend.getId(), backend);
        for (int i = 0; i < VIRTUAL_NODES; i++) {
            String virtualNodeName = backend.getId().toString() + "::" + String.valueOf(i);
            Long hashCode = new Long((long)virtualNodeName.hashCode());
            virtualNodes.put(hashCode, backend);
        }
    }

    public boolean exitsBackend(Long id) {
        return realNodes.contains(id);
    }

    public List<Backend> getAllRealNode(){
        List<Backend> beList = Lists.newArrayList();
        for(Long id : realNodes){
            Backend be = virtualNodes.get(id);
            beList.add(be);
        }
        return beList;
    }
}
