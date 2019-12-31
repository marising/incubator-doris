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

#include <gtest/gtest.h>
#include <boost/shared_ptr.hpp>
#include "util/cpu_info.h"
#include "runtime/cache/result_cache.h"
#include "runtime/buffer_control_block.h"
#include "gen_cpp/PaloInternalService_types.h"

namespace doris {

class ResultCacheTest : public testing::Test {
public:
    ResultCacheTest() {

    }
    virtual ~ResultCacheTest() {
    }
protected:
    virtual void SetUp() {
    }

private:
    void init_default(){
        this.init(16,4);
    }
    void init(int max_size, int ela_size);
    void clear();
    PCacheStatus init_batch_data(int sql_num,int part_num,int batch_num);
    ResultCache* _cache;
    PUpdateCacheRequest* _update_request;
    PUpdateCacheResult* _update_response
    PFetchCacheRequest* _fetch_request;
    PFetchCacheResult* _fetch_response;
};

void ResultCacheTest::init(int max_size, int ela_size){
    _cache = new ResultCache(max_size, ela_size);
    _update_request = new PUpdateCacheRequest();
    _update_response = new PUpdateCacheResult();
    _fetch_request = new PFetchCacheRequest();
    _fetch_response = new PFetchCacheResult();
}

void ResultCacheTest::clear(){
    SAFE_DELETE(_cache);
    SAFE_DELETE(_update_request);
    SAFE_DELETE(_update_response);
    SAFE_DELETE(_fetch_request);
    SAFE_DELETE(_fetch_response);
}

PCacheStatus ResultCacheTest::init_batch_data(int sql_num,int part_num,int batch_num) {
    //sql_id
    for (int i = 1; i < sql_num + 1; i++) {
        PUniqueId sql_id;
        sql_id.lo = i;
        sql_id.hi = i;
        request->set_sql_id(sql_id);
        //partition
        for (int j = 1; j < part_num + 1; j++) {
            PUpdateCacheValue* value = request->add_value();
            value->set_partition_key(j);
            value->set_last_version(j);
            value->last_version_time(j);
            //row batch size
            for(int k = 1; k < batch_num + 1; k++) {
                PRowBatch* batch = value->add_row_batch();
                batch->set_tuple_data("0123456789abcdef"); //16 byte
            }
        }
    }
    return cache.update(&request, response);
}

TEST_F(ResultCacheTest, update_data) {
    this.init_default();
    PCacheStatus st = init_batch_data(1, 1, 1);
    ASSERT_TRUE(st == PCacheStatus::UPDATE_SUCCESS);
}

TEST_F(ResultCacheTest, fetch_simple_data) {
    this.init_default();
    PCacheStatus st = init_batch_data(1, 1, 1);
    PUniqueId sql_id;
    sql_id.lo = 1;
    sql_id.hi = 1;
    _fetch_request->set_sql_id(sql_id);
    PFetchCacheParam* p1 = _fetch_request->add_param();
    p1->set_partition_key(1);
    p1->set_last_version(1);
    p1->last_version_time(1);
    cache.fetch(_fetch_request, _fetch_result);

    ASSERT_TRUE(_fetch_result->status() == PCacheStatus::FETCH_SUCCESS);
    ASSERT_EQ(_fetch_result->row_batch_size(), 1);
    this.clear();
}

TEST_F(ResultCacheTest, fetch_not_sqlid) {
     this.init_default();
    PCacheStatus st = init_batch_data(1, 1, 1);
    PUniqueId sql_id;
    sql_id.lo = 2;
    sql_id.hi = 2;
    _fetch_request->set_sql_id(sql_id);
    PFetchCacheParam* p1 = _fetch_request->add_param();
    p1->set_partition_key(1);
    p1->set_last_version(1);
    p1->last_version_time(1);
    cache.fetch(_fetch_request, _fetch_result);
    ASSERT_TRUE(_fetch_result->status() == PCacheStatus::NO_SQL_KEY);
    this.clear();
}

TEST_F(ResultCacheTest, fetch_range_data) {
    this.init_default();
    PCacheStatus st = init_batch_data(1, 3, 1);
    PUniqueId sql_id;
    sql_id.lo = 1;
    sql_id.hi = 1;
    _fetch_request->set_sql_id(sql_id);
    PFetchCacheParam* p1 = _fetch_request->add_param();
    p1->set_partition_key(2);
    p1->set_last_version(2);
    p1->last_version_time(2);
    PFetchCacheParam* p2 = _fetch_request->add_param();
    p1->set_partition_key(3);
    p1->set_last_version(3);
    p1->last_version_time(3);
    cache.fetch(_fetch_request, _fetch_result);

    ASSERT_TRUE(_fetch_result->status() == PCacheStatus::FETCH_SUCCESS);
    ASSERT_EQ(_fetch_result->row_batch_size(), 2);
    this.clear();
}

TEST_F(ResultCacheTest, fetch_invalid_key_range) {
     this.init_default();
    PCacheStatus st = init_batch_data(1, 3, 1);
    PUniqueId sql_id;
    sql_id.lo = 1;
    sql_id.hi = 1;
    _fetch_request->set_sql_id(sql_id);
    PFetchCacheParam* p1 = _fetch_request->add_param();
    p1->set_partition_key(2);
    p1->set_last_version(2);
    p1->last_version_time(2);
    cache.fetch(_fetch_request, _fetch_result);
    ASSERT_TRUE(_fetch_result->status() == PCacheStatus::NO_SQL_KEY);
    this.clear();
}

TEST_F(ResultCacheTest, fetch_not_match_version) {
    this.init_default();
    PCacheStatus st = init_batch_data(1, 1, 1);
    PUniqueId sql_id;
    sql_id.lo = 1;
    sql_id.hi = 1;
    _fetch_request->set_sql_id(sql_id);
    PFetchCacheParam* p1 = _fetch_request->add_param();
    p1->set_partition_key(1);
    p1->set_last_version(2);
    p1->set_last_version(2);
    cache.fetch(_fetch_request, _fetch_result);

    ASSERT_TRUE(_fetch_result->status() == PCacheStatus::FETCH_SUCCESS);
    ASSERT_EQ(_fetch_result->row_batch_size(), 0);
    this.clear();
}

TEST_F(ResultCacheTest, prune_data) {
    this.init(2,1);
    PCacheStatus st = init_batch_data(4, 128, 256); // (12+16+4)*256*128*4 = 4M
    ASSERT_LT(_cache->get_cache_size(), 3*1024*1024); //cache_size must less 3M
    this.clear();
}

TEST_F(ResultCacheTest, cache_clear) {
    this.init_default();
    PCacheStatus st = init_batch_data(1, 1, 1);
    _cache->clear();
    ASSERT_EQ(_cache->get_cache_size(),0); 
    this.clear();
}

}

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    // doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    return RUN_ALL_TESTS();
}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
