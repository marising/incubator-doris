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

#ifndef DORIS_BE_SRC_OLAP_CACHE_UTILS_H
#define DORIS_BE_SRC_OLAP_CACHE_UTILS_H

#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <map>
#include <list>
#include <iostream>
#include <exception>
#include <algorithm>
#include <sys/time.h>

namespace doris {

long get_current_second() {  
   struct timeval tv;  
   gettimeofday(&tv,NULL);  
   return tv.tv_sec;  
}

struct SqlStat{
	static uint32 DAY_SECONDS = 24 * 3600;
	long cache_time;
	long last_read_time;	
	uint32 read_count;
	SqlStat(){
		this.reset();
	}
	void reset(){
		cache_time = get_current_second();
		last_read_time = 0;
		read_count = 0;
	}
	void set_update(){
		cache_time = get_current_second();
		last_read_time = cache_time;
		read_count = 1;
	}
	void set_read(){
		last_read_time = get_current_second();
		read_count ++;
	}
	double last_read_day(){
		return (get_current_second() - last_read_time) * 1.0 / DAY_SECONDS;
	}
	double avg_read_count(){
		return read_count * DAY_SECONDS * 1.0 / (get_current_second() - last_read_time);
	}
}

/*
* Set cache information of table/partition, statistics cache usage information
*/
struct PartitionStat : SqlStat{
	int64 last_version;
	long last_version_time;
	CacheStat() {		
		this.reset();
	}
	void reset(){
		super.reset();
		last_version = 0;
		last_version_time = 0;		
	}
	bool check_match(int64& last_ver, long& last_ver_time){
		if( last_ver > last_version ) return false;
		if( last_ver_time > last_version_time ) return false;
		return true;
	}
	void set_update(int64& last_ver, long& last_ver_time){
		last_version = last_ver;
		last_version_time = last_ver_time;
		super.set_update();
	}
}

/*
struct RequestPartitionParam{
	int64 partition_ID;
	int64 last_version;
	int64 last_version_time;
}
typedef List<RequestPartitionParam> RequestPartitionParamList;

struct RequestCacheParams{
	TUniqueId sql_Key;
	RequestPartitionParamList param_list;
}

struct UpdatePartitionValue{
	uint32 partition_ID;
	uint32 last_version;
	long last_version_time;
	RowBatch* row_batch;
}
typedef List<UpdatePartitionValue> UpdatePartitionValueList;

struct UpdateCacheValues{
	TUniqueId sql_key;
	UpdatePartitionValueList value_list;
}
*/

}

#endif //DORIS_BE_SRC_OLAP_CACHE_UTILS_H