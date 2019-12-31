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

#include "runtime/cache/result_cache.h"

namespace doris {

void ResultCache::update(const PUpdateCacheValue* request, PUpdateCacheResult* response) {    
    ResultNode* node;
    PCacheStatus status;
    size_t update_size = 0;
    bool update_first = false;
    std::unique_lock<std::shared_mutex> write_lock(_cache_mtx);
    auto it = _cache_map.find(request->get_sql_key());
    if (it != _cache_map.end()) {
        node = iter->second;
        status = node->update_batches(request, update_size, update_first);
        if (update_first){
            _node_list->move_tail(node);
        }
    } else {
        node = _node_list->new_node();
        status = node->update_batches(request, update_size, update_first);
        _node_list->push(node);
        _cache_map[request->get_sql_key()] = node;                
    }
    _cache_size += update_size;
    response.set_status(status);
    this.prune();
}

void ResultCache::fetch(const PFetchCacheRequest* request, PFetchCacheResult* result) {
    std::shared_lock<std::shared_mutex> read_lock(_cache_mtx);
    CacheMapType::iterator it = _cache_map.find(request.get_sql_key());
    if (it == _cache_map.end()) {
        result->set_status(PCacheStatus::NO_SQL_KEY);
        return;
    }
    ResultNode* node = it->second->value;
    PartitionRowBatchList part_rowbatch_list;
    bool hit_first = false;
    PCacheStatus status = node->get_batches(request, part_rowbatch_list, hit_first);
    if (hit_first) {
        _node_list.move_tail(it->second);
    }
    result->set_status(status);
    for(PartitionRowBatchList::iterator it = part_rowbatch_list.begin(); it != part_rowbatch_list.end(); it++) {
        PFetchCacheValue* row = result->add_value();
        row->set_partition_id(it->get_partition_id());
        row->set_row_batch(it->get_row_batch());
    }
}

bool ResultCache::contains(const PUniqueId& sql_key) {
    std::shared_lock<std::shared_mutex> read_lock(_cache_mtx);
    return _node_map.find(key) != _node_map.end();
}

ResultNode* find_min_time_node(ResultNode* result_node*){
    if (result_node->get_prev()) {
        if (result_node->get_prev()->get_first_batch_last_time() <= result_node->get_first_batch_last_time()) {
            return result_node->get_prev();
        }        
    }
    if (result_node->get_next()) {
        if(result_node->get_next()->get_first_batch_last_time() < result_node->get_first_batch_last_time()){
            return result_node->get_next();
        }
    }
    return result_node;
}

/*
* Two-dimensional array, prune the min last_read_time PartitionRowBatch.
* The following example is the last read time array.
* Before:
*   1,2         //_head ResultNode*
*   1,2,3,4,5   
*   2,4,3,6,8   
*   5,7,9,11,13 //_tail ResultNode*
* After:
*   4,5         //_head
*   4,3,6,8
*   5,7,9,11,13 //_tail
*/
void ResultCache::prune() {
    if (_cache_size > (_max_size + _elasticity_size)) {
        ResultNode* result_node = _node_list->get_head();
        while (_cache_size > _max_size) {
            if (result_node == NULL) {
                break;
            }
            result_node = find_min_time_node(result_node);            
            _cache_size -= result_node->prune_first();
            if (result_node->get_batch_size() == 0) {              
                ResultNode* next_node;
                if (result_node->get_next()) {
                    next_node = result_node->get_next();
                } else if(result_node->get_prev()) {
                    next_node = result_node->get_prev();
                }else{
                    next_node = _node_list->get_head();
                }
                this.remove(result_node);
                result_node = next_node;
            }
        }
    }
}

void ResultCache::remove(ResultNode* result_node){
    auto node_it = _node_map.find(result_node->get_sql_key());
    if (node_it != _node_map.end()) {
        _node_map.erase(node_it);
        _node_list.remove(result_node);
        _cache_size -= result_node->get_batch_byte_size();
        _node_list.delete_node(result_node);
    }
}

void ResultCache::clear(){
    std::unique_lock<std::shared_mutex> clear_lock(_cache_mtx);
    _node_list->clear();
    _node_map.clear();
    _cache_size = 0;
}

}