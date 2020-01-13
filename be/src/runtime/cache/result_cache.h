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

#ifndef DORIS_BE_SRC_RUNTIME_RESULT_CACHE_H
#define DORIS_BE_SRC_RUNTIME_RESULT_CACHE_H

#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <map>
#include <list>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <boost/thread.hpp>
#include <exception>
#include "common/config.h"
#include "runtime/mem_tracker.h"
#include "runtime/mem_pool.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "runtime/cache/cache_utils.h"
#include "runtime/cache/result_node.h"

namespace doris {

/*
Struct SqlKey{
    int64 hi;
    int64 lo;
    bool operator== (const SqlKey& k) const{
        return hi == k.hi && lo == p.lo; 
    }
}

bool PUniqueId::operator == (const PUniqueId id) const {
    return hi() == id.hi() && lo() == id.lo();
}
*/

class ResultCache {
public:	
	ResultCache(int32 max_size, int32 elasticity_size) {
		_max_size = max_size * 1024 * 1024;
		_elasticity_size = elasticity_size * 1024 * 1024;
	}
	virtual ~ResultCache() {
	}
	void update(const PUpdateCacheRequest* request, PUpdateCacheResult* response);
	void fetch(const PFetchCacheRequest* request, PFetchCacheResult* result);
	bool contains(const UniqueId& sql_key);
	void clear();
	size_t get_cache_size(){
		return _cache_size;
	}
private:
	void prune();
	void remove(ResultNode* result_node);

	//At the same time, multithreaded reading
	//Single thread updating and cleaning(only single be, Fe is not affected)
	mutable boost::shared_mutex _cache_mtx;
	ResultNodeMap _node_map;
	ResultNodeList _node_list;	
	size_t _cache_size;
	size_t _max_size;
	double _elasticity_size;

private:
	ResultCache();
	ResultCache(const ResultCache&);
	const ResultCache& operator =(const ResultCache&);
};

}

#endif //DORIS_BE_SRC_RUNTIME_RESULT_CACHE_H
