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

namespace java com.jd.olap.thrift

enum TJDOlapOperationStatusCode {
    // Submitted to job queue. Hive Load is a heavy task, especially in the query phase with Spark.
    // Hive broker will try to limit overall task count or resource consumption.
    SUBMITTED_PENDING = 0;
    // Submitted hive load job and its spark app.
    SUBMITTED_RUNNABLE = 1;
    FINISHED_SUCCESSFULLY = 2;

    // Various failure reasons. Please check status message/payload for exact reasons.
    // If those spark app or stream load job are started, then we can check their status with existing facilities
   
    FAILURE_START_SPARK_APP = -1;
    FAILURE_USER_CANCELLED_BEFORE_START = -2 // User cancelled before start
    FAILURE_USER_ABORTED = -3 // User aborted a running task
    FAILURE_JOB_NOT_FOUND = -4 // Status code, in case the client try to operate a  non-existed job. Probably incorrect label specified, or it's submiitted on another broker
    FAILURE_OTHER = -99 // Could be various, such spark SQL exception during execution or during steam load progress.
}

// all string types for simplicity currently
struct TJDOLapLoadRequest {
    1: required string hiveLoadLabel; // End user specified label for a hive load job
    2: required string sql;
    3: required string targetHost;
    4: required string targetPort;
    5: required string targetDB;
    6: required string targetTable;
    7: required string olapUser;
    8: required string olapPasswd;
    9: optional string columnSeparator;
    10: optional string maxFilterRatio;
    11: optional string sparkMarket;
    12: optional string sparkProduction;
    13: optional string sparkQueue;
    14: optional string beeUser;
    15: optional string user;
    16: optional string hadoopUserCertificate;
} 

struct TJDOlapLoadStatus {
    1: required string hiveLoadLabel; // End user specified label for a hive load job
    2: required TJDOlapOperationStatusCode opStatus;
    3: optional string sparkAppId; // could be empty if a hive load job is pending for starting
    4: optional string streamloadJobLabel; // As agreed with management UI, this will be a value mapped by hiveLoadLabel
    // only state code is inadequate for various status and cause. It could be more than a message
    5: optional string message;  
    6: optional string payload; 
}

service TJDOlapLoadService {

    TJDOlapLoadStatus startHiveLoad(1:TJDOLapLoadRequest request);
    
    TJDOlapLoadStatus getStatus(1:string hiveLoadLabel);

    TJDOlapLoadStatus cancelHiveLoad(1:string hiveLoadLabel);
}
