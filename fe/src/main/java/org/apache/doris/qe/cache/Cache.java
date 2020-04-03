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

import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.qe.RowBatch;
import org.apache.doris.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class Cache {
    private static final Logger LOG = LogManager.getLogger(Cache.class);

    protected TUniqueId queryId;
    protected SelectStmt selectStmt;
    protected RowBatchBuilder rowBatchBuilder;
    protected CacheAnalyzer.CacheTable latestTable;

    protected CacheProxy proxy;

    protected Cache(TUniqueId queryId, SelectStmt selectStmt) {
        this.queryId = queryId;
        this.selectStmt = selectStmt;
        proxy = CacheProxy.getCacheProxy(CacheProxy.CacheProxyType.BE);
    }

    public abstract CacheProxy.FetchCacheResult getCacheData(Status status);

    public abstract SelectStmt getRewriteStmt();

    public abstract void copyRowBatch(RowBatch rowBatch);

    public abstract void updateCache();

    protected boolean checkRowLimit() {
        if (rowBatchBuilder.getRowSize() > Config.cache_result_max_row_count) {
            LOG.info("can not be cached. rowbatch size {} is more than {}", rowBatchBuilder.getRowSize(),
                    Config.cache_result_max_row_count);
            return false;
        } else {
            return true;
        }
    }
}
