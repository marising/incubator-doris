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
#include "runtime/cache/cache_utils.h"
#include "runtime/cache/result_node.h"

namespace doris {

size_t PartitionRowBatch::set_row_batch(const int64& last_version, const long& last_version_time, PRowBatch* prow_batch) {
	if (prow_batch == NULL) {
		return 0;
	}    
	SAFE_DELETE(_prow_batch);
	_prow_batch = PRowBatch.new();
	std::stringstream stream;
	prow_batch->SerializeToOstream(&stream);
	_row_batch->ParseFromIstream(&stream);
	_cache_stat.set_update(last_version, last_version_time);
	_batch_byte_size = RowBatch::get_batch_size(_row_batch);
	return _batch_byte_size;
}

bool PartitionRowBatch::is_hit_cache(const int64& partition_key, const int64& last_version, const long& last_version_time){
	if (partition_key != _partition_key) return false;
    if (!_cache_stat.check_match(last_version, last_version_time)) return false;
    _cache_stat.set_read();
	return true;
}

PCacheStatus ResultNode::update_batches(const PUpdateCacheRequest* request, int32& update_size, bool& update_first) {
    update_first = false;
	update_size = 0;
    if (!this.compare(request->get_sql_key())) {
	    return PCacheStatus::PARAM_ERROR;
    }
    if(request->value_size() >= 1024){
        return PCacheStatus::PARAM_ERROR;
    }
    if (_batch_list.size()) {
        update_first = true;
    }
	for (int i = 0; i < request->value_size(); i++) {
        const PUpdateCacheValue& value= request->value(i);
        int64 partition_key = value.partition_key();
        PartitionRowBatch* part_node;
        PartitionRowBatchMap::itertor it = _batch_map.find(partition_key);        
        if (!update_first && partition_key <= _batch_list.begin()->get_partition_key()) {
            update_first = true;
        }
        if (it == _batch_map.end()) {
	        part_node = new PartitionRowBatch(partition_key);
			update_size += part_node->set_row_batch(value.last_version(), value.last_vertion_time(),value.row_batch());
            _batch_map[partition_key] = part_node;
			_batch_list.push(part_node);
        }else{     
            update_size -= it->second->get_batch_byte_size();       
            update_size += it->second->set_row_batch(partNode);
        }
	}
    _batch_list.sort(ResultNode());
    return PCacheStatus::UPDATE_SUCCESS
}

/*
* Only the range query of the key of the partition is supported, and the separated partition key query is not supported.
* Because a query can only be divided into two parts, part1 get data from cache, part2 fetch_data by scan node from BE.
* Partion cache : 20191211-20191215
* Hit cache parameter : [20191211 - 20191215], [20191212 - 20191214], [20191212 - 20191216],[20191210 - 20191215]
* Miss cache parameter: [20191210 - 20191216]
*/
PCacheStatus ResultNode::get_batches(const PFetchCacheRequest* request, PartitionRowBatchList& row_batch_list, bool& hit_first) {
    hit_first = false;
    if (request->param_size() == 0) {
        return PCacheStatus::PARAM_ERROR;
    }
    if (_batch_list.size() == 0){     
        return PCacheStatus::NO_PARTITION_KEY;
    }
    //PartitionRowBatchList::iterator hit_begin = _partition_batch_list.end();    
    //PartitionRowBatchList::iterator hit_end = _partition_batch_list.end();
    int begin_index = -1, 
    int end_index = -1;
    PartitionRowBatchList::iterator begin_it, end_it;
    bool find = false;
    for(int param_index = 0, PartitionRowBatchList::iterator batch_it = _batch_list.begin(); 
        param_index < request->param_size(), part_it != _batch_list.end();) {
        const PFetchCacheParam& param = request->param(param_index);
        if (!find) {
            while (param->partition_key() > batch_it->get_partition_key()) {
                part_it ++;
            }
            while (param->partition_key() < batch_it->get_partition_key()) {
                param_index ++;
            }
            if (param->partition_key() == batch_it->get_partition_key()) {
                find = true;
            }
        }
        if (find) {
            if (batch_it->is_hit_cache(param.partition_key(), param.last_version(), param.last_version_time())) {
                if (begin_index < 0) {
                    begin_index = param_index;
                }
                end_index = param_index;
                param_index ++;
                batch_it ++;
            }else{
                break;
            }
        }
    }
    // //[20191210 - 20191216] hit partition range [20191212-20191214],the sql will be splited to 3 part!
    if (begin_index != 0 && end_index != request.param_size()-1) {
        return PCacheStatus::INVALID_KEY_RANGE
    }
    if (begin_it == _batch_list.begin()) {
        hit_first = true;
    }
    while(true){
        row_batch_list.push(begin_it);
        if (begin_it == end_it) 
            break;
        begin_it++;
    }
    return PCacheStatus::FETCH_SUCCESS;
}

/*
* prune first partition result
*/
size_t ResultNode::prune_first(){
    size_t prune_size = 0;
    if (_batch_list.size() == 0) {
        return prune_size;
    }
    PartitionRowBatch* part_node = _batch_list.begin();
    prune_size = part_node->get_batch_byte_size();
    _batch_list.erase(_batch_list.begin());
    SAFE_DELETE(part_node);
   return prune_size;
}

/*
* Remove the tail node of link
*/
ResultNode* ResultNode::pop() {
    if (!_head) {
        return NULL;
    }
    ResultNode* old_head = _head;
    _head = _old_head->get_next();
    _old_head->unlink();		
    _node_size--;
    if (_node_size == 0) {
        _tail = NULL;
    }
    return old_head;
}

void ResultNode::move_tail(ResultNode* node){
    remove(node);
    push(node);
}
void ResultNode::remove(ResultNode* node) {
    if (node == _head) {
        _head = node->get_next();
    }
    if (node == _tail) {
        _tail = node->get_prev();
    }
    node->unlink();
    _node_size--;
}
void ResultNode::push(ResultNode* node) {
    node->unlink();
    if (!_head) {
        _head = node;
        _tail = node;
    } else {
        _tail->set_next(node);
        node->set_prev(_tail);
        _tail = node;
    }
    _node_size++;		
}

}