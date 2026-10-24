/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include <gtest/gtest.h>
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafka_mock.h>

#include "operators/reader/KafkaConnector.h"
#include "operators/reader/KafkaDataSource.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

namespace gluten {
namespace {

constexpr char kConnectorId[] = "kafka-poll-timeout-test";
constexpr char kTopic[] = "kafka-poll-timeout";
constexpr int64_t kPollTimeoutMs = 10;

} // namespace

class KafkaPollTimeoutTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    auto* conf = rd_kafka_conf_new();
    char error[512];
    ASSERT_EQ(rd_kafka_conf_set(conf, "test.mock.num.brokers", "1", error, sizeof(error)), RD_KAFKA_CONF_OK);
    producer_ = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error, sizeof(error));
    ASSERT_NE(producer_, nullptr) << error;
    cluster_ = rd_kafka_handle_mock_cluster(producer_);
    ASSERT_NE(cluster_, nullptr);
    ASSERT_EQ(rd_kafka_mock_topic_create(cluster_, kTopic, 1, 1), RD_KAFKA_RESP_ERR_NO_ERROR);
    // Keep broker responses outside both the configured timeout and the old 100
    // ms consume wait.
    ASSERT_EQ(rd_kafka_mock_broker_set_rtt(cluster_, 1, 1'000), RD_KAFKA_RESP_ERR_NO_ERROR);
  }

  void TearDown() override {
    if (producer_ != nullptr) {
      rd_kafka_destroy(producer_);
    }
  }

  std::unique_ptr<KafkaDataSource> source() {
    auto type = ROW({"offset"}, {BIGINT()});
    connector::ColumnHandleMap columns{{"offset", std::make_shared<KafkaColumnHandle>("offset", BIGINT())}};
    return std::make_unique<KafkaDataSource>(type, columns, pool());
  }

  std::shared_ptr<KafkaConnectorSplit> split(int64_t endOffset) {
    std::unordered_map<std::string, std::string> params{
        {"bootstrap.servers", rd_kafka_mock_cluster_bootstraps(cluster_)}};
    return std::make_shared<KafkaConnectorSplit>(
        kConnectorId, kTopic, 0, 0, endOffset, kPollTimeoutMs, true, std::move(params));
  }

  rd_kafka_t* producer_{nullptr};
  rd_kafka_mock_cluster_t* cluster_{nullptr};
};

TEST_F(KafkaPollTimeoutTest, shortPollTimeoutBoundsConsumerWait) {
  int64_t minimumElapsedMs = std::numeric_limits<int64_t>::max();
  // Use the fastest of three independent readers so an isolated scheduler delay
  // cannot fail the test. Every old-code attempt waits at least 100 ms; the
  // configured timeout is only 10 ms.
  for (int attempt = 0; attempt < 3; ++attempt) {
    auto reader = source();
    reader->addSplit(split(1));
    ContinueFuture future = ContinueFuture::makeEmpty();
    const auto start = std::chrono::steady_clock::now();
    VELOX_ASSERT_THROW(reader->next(1, future), "Timed out after 10 ms fetching offset");
    const int64_t elapsedMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    minimumElapsedMs = std::min(minimumElapsedMs, elapsedMs);
    EXPECT_EQ(reader->getCompletedRows(), 0);
    EXPECT_EQ(reader->getCompletedBytes(), 0);
    // Consumer teardown is deliberately outside the measured poll interval.
  }
  RecordProperty("minimum_poll_elapsed_ms", std::to_string(minimumElapsedMs));
  EXPECT_LT(minimumElapsedMs, 80) << "A 10 ms poll must not use the fixed 100 ms consumer wait";
}

TEST_F(KafkaPollTimeoutTest, emptyRangeDoesNotWaitForBroker) {
  auto reader = source();
  reader->addSplit(split(0));
  ContinueFuture future = ContinueFuture::makeEmpty();
  EXPECT_EQ(reader->next(1, future).value(), nullptr);
  EXPECT_EQ(reader->getCompletedRows(), 0);
  EXPECT_EQ(reader->getCompletedBytes(), 0);
}

} // namespace gluten
