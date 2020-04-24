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

#include "runtime/routine_load/kafka_consumer_pipe.h"
#include "common/config.h"

#include <gtest/gtest.h>

#include "json/json.h"

namespace doris {

class KafkaConsumerPipeTest : public testing::Test {
public:
    KafkaConsumerPipeTest() {}
    virtual ~KafkaConsumerPipeTest() {}

    void SetUp() override {}

    void TearDown() override {}
};

TEST_F(KafkaConsumerPipeTest, append_read) {
    config::jdwdatafile = std::string(getenv("DORIS_HOME")) + "/conf/jdwdata.json";
    KafkaConsumerPipe k_pipe(1024 * 1024, 64 * 1024);

    std::string msg1 = "i have a dream";
    std::string msg2 = "This is from kafka";
    std::string msg3 = R"json({"a","i have a dream"})json";

    Status st;
    st = k_pipe.append_with_line_delimiter((char*)msg1.c_str(), msg1.length(), "");
    ASSERT_TRUE(st.ok());
    st = k_pipe.append_with_line_delimiter((char*)msg2.c_str(), msg2.length(), "");
    ASSERT_TRUE(st.ok());
    st = k_pipe.append_with_line_delimiter((char*)msg3.c_str(), msg3.length(), "json");
    ASSERT_TRUE(st.ok());
    st = k_pipe.finish();
    ASSERT_TRUE(st.ok());

    char buf[1024] = {0};
    size_t data_size = 1024;
    bool eof = false;
    st = k_pipe.read((uint8_t*)buf, &data_size, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(data_size, msg1.length() + msg2.length() + msg3.length() + 3);
    ASSERT_EQ(eof, false);

    data_size = 1024;
    st = k_pipe.read((uint8_t*)buf, &data_size, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(data_size, 0);
    ASSERT_EQ(eof, true);
}

TEST_F(KafkaConsumerPipeTest, avro_json_append_read) {
    config::jdwdatafile = std::string(getenv("DORIS_HOME")) + "/conf/jdwdata.json";
    KafkaConsumerPipe k_pipe_json(1024 * 1024, 64 * 1024);

    std::string savrojson = R"json(
{
    "mid":"mm",
    "db":"kawmsidc1_report",
    "sch":"kawmsidc1_report",
    "tab":"ob_locate_d",
    "opt":"UPDATE",
    "ts":"mmm",
    "ddl":"mmm",
    "err":"mmm",
    "src":{
        "title":"i have a dream "
    },
    "cur":{
        "title":"\"\\\b\f\n\r\t'"
    },
    "cus":{
    }
})json";
    std::string savrojsonconverted = R"json({"title":"\"\\\b\f\n\r\t'"}
)json";

    Status st;
    st = k_pipe_json.append_with_line_delimiter((char*)savrojson.c_str(), savrojson.length(),
                                                "avro_json");
    ASSERT_TRUE(st.ok());
    st = k_pipe_json.finish();
    ASSERT_TRUE(st.ok());

    char buf[1024] = {0};
    size_t data_size = 1024;
    bool eof = false;
    st = k_pipe_json.read((uint8_t*)buf, &data_size, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(0 == strcmp(buf, savrojsonconverted.c_str()));
    ASSERT_EQ(eof, false);

    data_size = 1024;
    st = k_pipe_json.read((uint8_t*)buf, &data_size, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(data_size, 0);
    ASSERT_EQ(eof, true);
}

TEST_F(KafkaConsumerPipeTest, json_value_to_quotedstring) {
    // json valueToQuotedString test
    Json::Value root;
    Json::Reader reader;
    const std::string escapejson = "{\"key\":" + Json::valueToQuotedString("\"\\\b\f\n\r\t'") + "}";
    ASSERT_TRUE(reader.parse(escapejson, root));
}

} // namespace doris

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    return RUN_ALL_TESTS();
}

