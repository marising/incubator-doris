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

#ifndef DORIS_BE_SRC_RUNTIME_RESULT_NODE_H
#define DORIS_BE_SRC_RUNTIME_RESULT_NODE_H

#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <map>
#include <list>
#include <string>
#include <iostream>
#include <exception>
#include <algorithm>
#include <sys/time.h>
#include "common/config.h"
#include "util/uid_util.h"
#include "olap/olap_define.h"
#include "runtime/mem_pool.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "runtime/cache/cache_utils.h"

namespace doris {

enum PCacheStatus;
class PFetchCacheParam;
class PFetchCacheRequest;
class PFetchCacheValue;
class PFetchCacheResult;
class PUpdateCacheValue;
class PUpdateCacheRequest;
class PUpdateCacheResult;

/*
* Cache one partition data
*/
class PartitionRowBatch{
public:
	PartitionRowBatch(int64 partition_key) : _partition_key(partition_key),  _prow_batch(NULL), _data_size(0) {
	}

	~PartitionRowBatch() {
	}
	
	size_t set_row_batch(const int64& last_version, const long& last_version_time, const PRowBatch* prow_batch);
	bool is_hit_cache(const int64& partition_key, const int64& last_version, const long& last_version_time);
	void clear();

	int64 get_partition_key() const {
		return _partition_key;
	}

	PRowBatch* get_row_batch() {
		return _prow_batch;
	}

	size_t get_data_size() {
		return _data_size;
	}

	const PartitionStat* get_stat() const{
		return &_cache_stat;
	}

private:
	int64 _partition_key;
	PRowBatch* _prow_batch;
	size_t _data_size;
	PartitionStat _cache_stat;
};

typedef std::list<PartitionRowBatch*> PartitionRowBatchList;
typedef boost::unordered_map<int64, PartitionRowBatch*> PartitionRowBatchMap;	

/*
* Cache the result of one select statment
*/
class ResultNode {
public:
    ResultNode() : _sql_key(0,0), _prev(NULL), _next(NULL), _data_size(0) {
    }

	ResultNode(const UniqueId& sql_key) : _sql_key(sql_key), _prev(NULL), _next(NULL), _data_size(0) {	
	}

	virtual ~ResultNode() {
	}

	void init() {
		clear();
	}

	PCacheStatus update_partition(const PUpdateCacheRequest* request, bool& update_first);
	PCacheStatus get_partition(const PFetchCacheRequest* request, PartitionRowBatchList& rowBatchList, 
        bool& hit_first);
	size_t prune_first();
	void clear();

	bool operator()(const ResultNode* left_node,const ResultNode* right_node) {
		if (left_node->get_partition_count() == 0) {
			return true;
		}
		if (right_node->get_partition_count() == 0) {
			return false;
		}
		return left_node->get_first_stat()->last_read_time < right_node->get_first_stat()->last_read_time;
	}

	ResultNode* get_prev() {
		return _prev;
	}

	void set_prev(ResultNode* prev) {
		_prev = prev;
	}

	ResultNode* get_next() {
		return _next;
	}

	void set_next(ResultNode* next) {
		_next = next;
	}

    void append(ResultNode* tail);

	void unlink();
	
    size_t get_partition_count() const {
		return _partition_list.size();
	}

    size_t get_data_size() const {
        return _data_size;
    }

	UniqueId get_sql_key() {
		return _sql_key;
	}

	bool sql_key_null() {
		return _sql_key.hi == 0  && _sql_key.lo == 0;
	}

	void set_sql_key(const UniqueId& sql_key) {
		_sql_key = sql_key;
	}

	long first_partition_last_time() const {
		if (_partition_list.size() == 0) {
			return 0;
		} else {
            const PartitionRowBatch* first = *(_partition_list.begin());
			return first->get_stat()->last_read_time;
		}
	}

	const PartitionStat* get_first_stat() const {
		if( _partition_list.size() == 0) {
			return NULL;
		} else {
			return (*(_partition_list.begin()))->get_stat();
		}
	}

	const PartitionStat* get_last_stat() const{
		if (_partition_list.size() == 0) {
			return NULL;
		} else {
			return (*(_partition_list.end()--))->get_stat();
		}		
	}

private:
	UniqueId _sql_key;
	ResultNode* _prev;
	ResultNode* _next;
    size_t _data_size;
	PartitionRowBatchList _partition_list;
	PartitionRowBatchMap _partition_map;    
};

typedef std::unordered_map<UniqueId, ResultNode*> ResultNodeMap;

// a doubly linked list class
class ResultNodeList {
public:	
	ResultNodeList() : _head(NULL), _tail(NULL), _node_count(0) {
	}
	virtual ~ResultNodeList() {
	}	
	
    ResultNode* new_node(const UniqueId& sql_key) {
		return new ResultNode(sql_key);
	}	

	void delete_node(ResultNode** node) {
		SAFE_DELETE(*node);
	}       
    
	ResultNode* pop();
	void move_tail(ResultNode* node);
	//Just remove node from link, do not delete node
	void remove(ResultNode* node);
	void push(ResultNode* node);
	void clear();

	ResultNode* get_head() const{
		return _head;
	}

	ResultNode* get_tail() const {
		return _tail;
	}

    size_t get_node_count() const {
        return _node_count;
    }
private:
	ResultNode* _head;
	ResultNode* _tail;
	size_t _node_count;
};

}

#endif //DORIS_BE_SRC_RUNTIME_CACHE_ROW_H
