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

#ifndef DORIS_BE_SRC_RUNTIME_CACHE_UTILS_H
#define DORIS_BE_SRC_RUNTIME_CACHE_UTILS_H

#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <map>
#include <list>
#include <iostream>
#include <exception>
#include <algorithm>
#include <sys/time.h>
#include <gutil/integral_types.h>
#include <shared_mutex>
#include <boost/thread.hpp>

namespace doris {

typedef boost::shared_lock<boost::shared_mutex> CacheReadLock;
typedef boost::unique_lock<boost::shared_mutex> CacheWriteLock;

#ifndef PARTITION_CACHE_DEV
#define  PARTITION_CACHE_DEV
#endif

struct SqlStat {
	static const uint32 DAY_SECONDS = 86400;
	long cache_time;
    long last_update_time;
	long last_read_time;	
	uint32 read_count;
	SqlStat() {
        init();
	}    

    inline long cache_time_second() {  
       struct timeval tv;  
       gettimeofday(&tv,NULL);  
       return tv.tv_sec;  
    }

    void init() {
        cache_time = 0;
        last_update_time = 0;
        last_read_time = 0;
        read_count = 0;
    }

	void set_update() {
		last_update_time = cache_time_second();
        if (cache_time == 0) {
            cache_time = last_update_time;
        }
		last_read_time = last_update_time;
        read_count++;
	}

	void set_read() {
		last_read_time = cache_time_second();
		read_count++;
	}

	double last_read_day() {
		return (cache_time_second() - last_read_time) * 1.0 / DAY_SECONDS;
	}

	double avg_read_count() {
		return read_count * DAY_SECONDS * 1.0 / (cache_time_second() - last_read_time + 1);
	}
};

/*
* Set cache information of table/partition, statistics cache usage information
*/
struct PartitionStat : SqlStat {
	int64 last_version;
	long last_version_time;
	PartitionStat() {
        SqlStat::init();
        last_version = 0;
        last_version_time = 0;
	}

	bool check_match(const int64& last_ver, const long& last_ver_time) {
		if (last_ver > last_version) return false;
		if (last_ver_time > last_version_time) return false;
		return true;
	}
        
    bool check_newer(const int64& last_ver, const long& last_ver_time) {
        if (last_ver == 0 || last_ver_time == 0) {
            return true;
        }
        return (last_ver_time > last_version_time) && (last_ver > last_version);
    }

	void set_update(const int64& last_ver, const long& last_ver_time) {
		SqlStat::set_update();
		last_version = last_ver;
		last_version_time = last_ver_time;		
	}
};

}
#endif //DORIS_BE_SRC_RUNTIME_CACHE_UTILS_H
