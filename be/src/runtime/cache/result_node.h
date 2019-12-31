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

#ifndef DORIS_BE_SRC_OLAP_RESULT_NODE_H
#define DORIS_BE_SRC_OLAP_RESULT_NODE_H

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
#include "runtime/mem_pool.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "runtime/cache/cache_utils.h"

namespace doris {

class PCacheStatus;
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
	PartitionRowBatch(int64 partition_key):_partition_key(partition_key),  _row_batch(NULL), _batch_byte_size(0) {
	}
	~PartitionRowBatch(){	
		this.clear();
	}
	
	size_t set_row_batch(const int64& last_version, const long& last_version_time, PRowBatch* prow_batch);			
	bool is_hit_cache(const int64& partition_key, const int64& last_version, const long& last_version_time);

	uint32 get_partition_key(){
		return _partition_key;
	}	
	RowBatch* get_row_batch(){
		return _prow_batch;
	}
	size_t get_batch_size(){
		return _batch_byte_size;
	}
	bool operator()(const PartitionRowBatch* left_node,const PartitionRowBatch* right_node) {
		return left_node->get_partition_key() < right_node->get_partition_key();
	}
	const CacheState& get_stat() {
		return _cache_stat;
	}
	void clear() {
		SAFE_DELETE(_prow_batch);
		_partition_key = 0;
		_batch_byte_size = 0;		
		_cache_stat.reset();
	}
private:
	int64 _partition_key;
	PRowBatch* _prow_batch;
	size_t _batch_byte_size;
	PartitionStat _cache_stat;
}

typedef List<PartitionRowBatch*> PartitionRowBatchList;
typedef boost::unordered_map<int64, PartitionRowBatch*> PartitionRowBatchMap;	

/*
* Cache the result of one select statment
*/
class ResultNode {
public:
	ResultNode(PUniqueId& sql_key) : _prev(NULL), _next(NULL) {	
		set_sql_key(sql_key);
	}
	virtual ~ResultNode() {
		clear();
	}

	void init(){
		clear();
		_partition_batch_list = new PartitionRowBatchList();
	}
	void clear(){
		SAFE_DELETE(_partition_batch_list);
		_prev = NULL;
		_next = NULL;
		_sqlKey.hi = 0;
		_sqlKey.lo = 0;		
	}
	PCacheStatus update_batches(const PUpdateCacheRequest* request, int32& update_size, bool& update_first);
	PCacheStatus get_batches(const PFetchCacheRequest* request, List<PartitionRowBatch*>& rowBatchList, bool& hit_first);
	size_t prune_first();

	bool operator()(const ResultNode* left_node,const ResultNode* right_node) {
		if (left_node->get_batch_size() == 0) {
			return true;
		}
		if (right_node->get_batch_size() == 0) {
			return false;
		}
		return left_node->get_first_stat().last_read_time < right_node->get_first_stat().last_read_time;
	}
	ResultNode* get_prev(){
		return _prev;
	}
	void set_prev(ResultNode* prev){
		_prev = prev;
	}
	ResultNode* get_next(){
		return _next;
	}
	void set_next(ResultNode* next){
		_next = next;
	}
	void unlink() {
		if (_next) {
			_next->set_prev(_prev);
		}
		if (_prev) {
			_prev->set_next(_next);
		}
		_next = NULL;
		_prev = NULL;
	}
	size_t get_batch_byte_size(){
		return _batch_byte_size;
	}
	PUniqueId get_sql_key(){
		return _sql_key;
	}
	bool sql_key_null(){
		return (_sql_key.hi == 0  && _sql_key.lo == 0);
	}
	bool compare(PUniqueId& sql_key){
		return (_sql_key.hi == sql_key.hi && _sql_key.lo == sql_key.lo);
	}
	void set_sql_key(PUniqueId& sql_key){
		_sql_key.hi = sql_key.hi;
		_sql_key.lo = sql_key.lo;
	}
	size_t get_batch_size(){
		return _batch_list.size();
	}
	long get_first_batch_last_time(){
		if (_batch_list.size() == 0) {
			return 0;
		} else {
			return _batch_list.begin()->get_stat()->last_read_time;
		}
	}
	const PartitionStat* get_first_stat(){
		if( _batch_list.size() == 0){
			return NULL;
		} else {
			return _batch_list.begin()->get_stat();
		}
	}
	const PartitionStat* get_last_stat(){
		if (_batch_list.size() == 0) {
			return NULL;
		} else {
			return _batch_list.back()->get_stat();
		}		
	}
private:
	ResultNode* _prev;
	ResultNode* _next;
	PUniqueId _sql_key;
	PartitionRowBatchList _batch_list;
	PartitionRowBatchMap _batch_map;
	//size_t _batch_byte_size;
};

typedef boost::unordered_map<int64, ResultNode*> ResultNodeMap;	

// a doubly linked list class
class ResultNodeList {
public:	
	ResultNodeList() : _head(NULL), _tail(NULL), _node_size(0) {
	}
	virtual ~ResultNodeList() {
		clear();
	}
	void clear() {
		_head = NULL;
		_tail = NULL;
		_node_size = 0;
		_memory_size = 0;
	}
	//TODO:Use object pool to manage ResultNode and PartitionRowBatch
	ResultNode* new_node(){
		return new ResultNode();
	}	
	void delete_node(ResultNode** node){
		SAFE_DELETE(*node);
	}
	ResultNode* pop();
	void move_tail(ResultNode* node);
	//Just remove node from link, do not delete node
	void remove(ResultNode* node);
	void push(ResultNode* node);
	ResultNode* get_head(){
		return _head;	
	}
	ResultNode* get_tail(){
		return _tail;
	}
private:
	//No node, _head = _tail = NULL
	//One node, _head = _tail
	ResultNode* _head;
	ResultNode* _tail;
	size_t _node_size;
	size_t _memory_size;
};

#endif //DORIS_BE_SRC_OLAP_CACHE_ROW_H