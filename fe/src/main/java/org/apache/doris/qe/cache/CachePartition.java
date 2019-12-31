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
import org.apache.doris.rpc.PFetchDataRequest;
import org.apache.doris.system.Backend;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class CachePartition {
    private static final Logger LOG = LogManager.getLogger(CachePartition.class);

    private static final int VIRTUAL_NODES = 10;
    private List<Long> realNodes = new LinkedList<Long>();
    private SortedMap<Long, Backend> virtualNodes = new TreeMap<Long, Backend>();
    private static CachePartition cachePartition;
    private MessageDigest msgDigest;

    public static CachePartition getInstance() {
        if (cachePartition == null) {
            cachePartition = new CachePartition();
        }
        return cachePartition;
    }

    protected CachePartition() throws NoSuchAlgorithmException {
        msgDigest = MessageDigest.getInstance("MD5");
    }


    /**
     * Using the consistent hash and the hi part of sqlkey to get the backend node
     * @param sqlKey 128 bit's sql md5
     * @return Backend
     */
    public Backend findBackend(PUniqueId sqlKey) {
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

    public void addBackends(ImmutableMap<Long, Backend> backends) {
        for (Map.Entry<Long, Backend> entry : backends.entrySet()) {
            Long backendID = entry.getKey();
            Backend backend = entry.getValue();
            addBackend(backendID, backend);
        }
    }

    public void addBackend(Long backendID, Backend backend) {
        realNodes.add(backendID);
        for (int i = 0; i < VIRTUAL_NODES; i++) {
            String virtualNodeName = backendID.toString() + "::" + String.valueOf(i);
            int hash = getHash(virtualNodeName);
            virtualNodes.put(hash, backend);
        }
    }
}
