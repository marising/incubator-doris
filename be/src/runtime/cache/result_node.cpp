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
#include "gen_cpp/internal_service.pb.h"
#include "runtime/cache/cache_utils.h"
#include "runtime/cache/result_node.h"

namespace doris {
	

bool compare_partition(const PartitionRowBatch* left_node, const PartitionRowBatch* right_node) {
    return left_node->get_partition_key() < right_node->get_partition_key();
}

//return new batch size,only include the size of PRowBatch
size_t PartitionRowBatch::set_row_batch(const int64& last_version, const long& last_version_time, 
    const PRowBatch* prow_batch) {

	if (prow_batch == NULL) {
        return _data_size;  
	} 
    if (!_cache_stat.check_newer(last_version, last_version_time)) {
        return _data_size;  
    }           
    SAFE_DELETE(_prow_batch);
	_prow_batch = new PRowBatch(*prow_batch);
	_cache_stat.set_update(last_version, last_version_time);
	_data_size = RowBatch::get_batch_size(*_prow_batch);
    //LOG(INFO) << "finish set row batch, row num:"<< prow_batch->num_rows() << ", batch size:" << _data_size;
	return _data_size;
}

bool PartitionRowBatch::is_hit_cache(const int64& partition_key, const int64& last_version, 
    const long& last_version_time) {

	if (partition_key != _partition_key) return false;
    if (!_cache_stat.check_match(last_version, last_version_time)) return false;
    _cache_stat.set_read();
	return true;
}

void PartitionRowBatch::clear() {
    LOG(INFO) << "clear partition rowbatch.";
    SAFE_DELETE(_prow_batch);
	_partition_key = 0;
	_data_size = 0;
    _cache_stat.init();
}

PCacheStatus ResultNode::update_partition(const PUpdateCacheRequest* request, bool& update_first) {
    update_first = false;
    if (_sql_key != request->sql_key()) {
        LOG(INFO) << "no match sql_key " << request->sql_key().hi()
            << request->sql_key().lo();
	    return PCacheStatus::PARAM_ERROR;
    }

    if (request->value_size() > config::cache_max_partition_count) {
        LOG(WARNING) << "too many partitions size:" << request->value_size();
        return PCacheStatus::PARAM_ERROR;
    }

    int64 first_key = kint64max; 
    if (_partition_list.size() == 0) {
        update_first = true;
    } else {
        first_key = (*(_partition_list.begin()))->get_partition_key();
    }
    PartitionRowBatch* partition = NULL;
	for (int i = 0; i < request->value_size(); i++) {
        const PUpdateCacheValue& value= request->value(i);
        int64 partition_key = value.partition_key();
        if (!update_first && partition_key <= first_key) {
            update_first = true;
        } 
        auto it = _partition_map.find(partition_key);
        if (it == _partition_map.end()) {
	        partition = new PartitionRowBatch(partition_key); 
            partition->set_row_batch(value.last_version(), value.last_version_time(), &value.row_batch());
            _partition_map[partition_key] = partition;
			_partition_list.push_back(partition);
            LOG(INFO) << "add index:" << i 
                << ", pkey:" << partition->get_partition_key() 
                << ", list size:" << _partition_list.size() 
                << ", map size:" << _partition_map.size();
        } else {
            partition = it->second;
            _data_size -= partition->get_data_size(); 
            partition->set_row_batch(value.last_version(), value.last_version_time(), &value.row_batch());
            LOG(INFO) << "update index:" << i 
                << ", pkey:" << partition->get_partition_key() 
                << ", list size:" << _partition_list.size() 
                << ", map size:" << _partition_map.size();
        }
        _data_size += partition->get_data_size();
	}
    _partition_list.sort(compare_partition);
    LOG(INFO) << "finish update batches:" << _partition_list.size();
    while (config::cache_max_partition_count > 0 && 
        _partition_list.size() > config::cache_max_partition_count) {
       prune_first(); 
    }
    return PCacheStatus::UPDATE_SUCCESS;
}

/*
* Only the range query of the key of the partition is supported, and the separated partition key query is not supported.
* Because a query can only be divided into two parts, part1 get data from cache, part2 fetch_data by scan node from BE.
* Partion cache : 20191211-20191215
* Hit cache parameter : [20191211 - 20191215], [20191212 - 20191214], [20191212 - 20191216],[20191210 - 20191215]
* Miss cache parameter: [20191210 - 20191216]
*/
PCacheStatus ResultNode::get_partition(const PFetchCacheRequest* request, PartitionRowBatchList& row_batch_list, 
    bool& hit_first) {

    hit_first = false;
    if (request->param_size() == 0) {
        return PCacheStatus::PARAM_ERROR;
    }
    if (_partition_list.size() == 0) {
        return PCacheStatus::NO_PARTITION_KEY;
    }
    
    bool find = false;
    int begin_idx = -1, end_idx = -1, param_idx = 0; 
    auto begin_it = _partition_list.end();
    auto end_it = _partition_list.end();
    auto part_it = _partition_list.begin();
    while (param_idx < request->param_size() && part_it != _partition_list.end()) {
//        LOG(LOG) << "Param index : " << param_idx 
//            << ", param part Key : "<< request->param(param_idx).partition_key()
//            << ", batch part key : " << (*part_it)->get_partition_key();
        if (!find) {
            while (part_it != _partition_list.end() && 
                request->param(param_idx).partition_key() > (*part_it)->get_partition_key()) {
                part_it++;
            }
            while (param_idx < request->param_size() && 
                request->param(param_idx).partition_key() < (*part_it)->get_partition_key()) {
                param_idx ++;
            }
            if (request->param(param_idx).partition_key() == (*part_it)->get_partition_key()) {
                find = true;
            }
        }
        if (find) {
//            LOG(WARNING) << "Find! Param index : " << param_idx 
//                << ", param part Key : "<< request->param(param_idx).partition_key()
//                << ", batch part key : " << (*part_it)->get_partition_key();
            if ((*part_it)->is_hit_cache(request->param(param_idx).partition_key(), 
                    request->param(param_idx).last_version(), 
                    request->param(param_idx).last_version_time())) {
                if (begin_idx < 0) {
                    begin_idx = param_idx;
                    begin_it = part_it;
                }
                end_idx = param_idx;
                end_it = part_it;
                param_idx++;
                part_it++;
            }else{
                return PCacheStatus::DATA_OVERDUE;    
            }
        }
    }

    if (begin_it == _partition_list.end() && end_it == _partition_list.end()) {
        return PCacheStatus::FETCH_SUCCESS;
    }
 
    //[20191210 - 20191216] hit partition range [20191212-20191214],the sql will be splited to 3 part!
    if (begin_idx != 0 && end_idx != request->param_size()-1) {
        return PCacheStatus::INVALID_KEY_RANGE;
    }
    if (begin_it == _partition_list.begin()) {
        hit_first = true;
    }
    while (true) {
        row_batch_list.push_back(*begin_it);
        if (begin_it == end_it){ 
            break;
        }
        begin_it++;
    }
    return PCacheStatus::FETCH_SUCCESS;
}

/*
* prune first partition result
*/
size_t ResultNode::prune_first(){
    if (_partition_list.size() == 0) {
        return 0;
    }
    PartitionRowBatch* part_node = *_partition_list.begin();
    size_t prune_size = part_node->get_data_size();
    _partition_list.erase(_partition_list.begin());
    SAFE_DELETE(part_node);
    _data_size -= prune_size;
    return prune_size;
}

void ResultNode::clear(){
    LOG(INFO) << "clear result node:" << _sql_key;
    _sql_key.hi = 0;
    _sql_key.lo = 0;
    for (auto it = _partition_list.begin(); it != _partition_list.end(); ) {
        (*it)->clear();
        delete *it;
        it = _partition_list.erase(it);
    }
    _data_size = 0;
}


void ResultNode::append(ResultNode* tail){
    _prev = tail;
    if (tail) tail->set_next(this);
}    

void ResultNode::unlink() {
    if (_next) {
	    _next->set_prev(_prev);
	}
    if (_prev) {
	    _prev->set_next(_next);
	}
	_next = NULL;
	_prev = NULL;
}

/*
* Remove the tail node of link
*/
ResultNode* ResultNodeList::pop() {
    remove(_head);
    return _head;
}

void ResultNodeList::remove(ResultNode* node) {
    if (!node) return;
    node->unlink();
    if (node == _head) _head = node->get_next();   
    if (node == _tail) _tail = node->get_prev();
    _node_count--;
}

void ResultNodeList::push(ResultNode* node) {
    if(!node) return;        
    if (!_head) _head = node;
    node->append(_tail);
    _tail = node;
    _node_count++;		
}

void ResultNodeList::move_tail(ResultNode* node){
    if (!node) return;
    if (node == _tail) return; 
    if (!_head)  
        _head = node;
    else if (node == _head) 
        _head = node->get_next();
    node->unlink();
    node->append(_tail);
    _tail = node;
}

void ResultNodeList::clear() {
    LOG(INFO) << "clear result node list.";
    while (_head) {
        ResultNode* tmp_node = _head->get_next();
        _head->clear();
        SAFE_DELETE(_head);
        _head = tmp_node;
    }
    _node_count = 0;
}

}
