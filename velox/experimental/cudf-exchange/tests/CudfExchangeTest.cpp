/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <cudf/column/column_factories.hpp>
#include <cudf/contiguous_split.hpp>
#include <cudf/table/table.hpp>
#include <cudf/types.hpp>
#include <folly/Executor.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>
#include <rmm/device_buffer.hpp>
#include <chrono>
#include <memory>
#include <sstream>
#include <vector>
#include "CudfTestHelpers.h"
#include "folly/experimental/EventCount.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/experimental/cudf-exchange/Communicator.h"
#include "velox/experimental/cudf-exchange/CudfOutputQueueManager.h"
#include "velox/experimental/cudf-exchange/tests/CudfPartitionedOutputMock.h"
#include "velox/experimental/cudf-exchange/tests/CudfTestData.h"
#include "velox/experimental/cudf-exchange/tests/CudfTestHelpers.h"
#include "velox/experimental/cudf-exchange/tests/SinkDriverMock.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::core;

namespace facebook::velox::cudf_exchange {

struct ExchangeTestParams {
  int numSrcDrivers;
  int numDstDrivers;
  int numPartitions;
  int numChunks;
  int numRowsPerChunk;
  int numUpstreamTasks;
};

// Helper function to generate test parameters with different numUpstreamTasks
static std::vector<ExchangeTestParams> generateTestParams() {
  std::vector<ExchangeTestParams> params;

  // Base configurations
  struct BaseConfig {
    const char* description;
    int numSrcDrivers;
    int numDstDrivers;
    int numPartitions;
    int numChunks;
    int numRowsPerChunk;
  };

  std::vector<BaseConfig> baseConfigs = {
      // Test to check end-2-end connectivity
      {"Simple", 1, 1, 1, 100, 1000 * 1000},
      // Test to check parallelism at source
      {"SourceDrivers", 10, 1, 1, 10, 1000 * 1000},
      // Test to check parallelism at source and sink
      {"SourceSinkDrivers", 10, 10, 1, 10, 1000}};

  // Generate variants with different number of upstream tasks.
  std::vector<int> upstreamTaskCounts = {1, 10};

  for (const auto& base : baseConfigs) {
    for (int numUpstream : upstreamTaskCounts) {
      params.push_back(
          {.numSrcDrivers = base.numSrcDrivers,
           .numDstDrivers = base.numDstDrivers,
           .numPartitions = base.numPartitions,
           .numChunks = base.numChunks,
           .numRowsPerChunk = base.numRowsPerChunk,
           .numUpstreamTasks = numUpstream});
    }
  }

  return params;
}

// Custom parameter name generator for readable test names
struct ExchangeTestParamsPrinter {
  std::string operator()(
      const ::testing::TestParamInfo<ExchangeTestParams>& info) const {
    const auto& p = info.param;
    std::ostringstream oss;
    oss << "Src" << p.numSrcDrivers << "_Dst" << p.numDstDrivers << "_Part"
        << p.numPartitions << "_Chunks" << p.numChunks << "_RowsPer"
        << p.numRowsPerChunk << "_Upstream" << p.numUpstreamTasks;
    return oss.str();
  }
};

class CudfExchangeTest : public testing::TestWithParam<ExchangeTestParams> {
 protected:
  static constexpr uint16_t kCommunicatorPort = 21346;
  static constexpr auto kUnusedCoordinatorUrl =
      std::string_view("http://localhost:12345/bla");

  static std::shared_ptr<CudfOutputQueueManager> queueManager_;
  static std::shared_ptr<std::thread> communicatorThread_;
  static std::shared_ptr<Communicator> communicator_;

  static void SetUpTestCase() {
    VLOG(0) << "setup test case, creating queue manager, communicator, etc..";
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});

    queueManager_ = CudfOutputQueueManager::getInstanceRef();
    ContinueFuture future;
    communicator_ = facebook::velox::cudf_exchange::Communicator::initAndGet(
        kCommunicatorPort, std::string(kUnusedCoordinatorUrl), &future);
    if (communicator_) {
      communicatorThread_ = std::make_shared<std::thread>(
          &facebook::velox::cudf_exchange::Communicator::run,
          communicator_.get());
    } else {
      ADD_FAILURE() << "Communicator initialization failed";
    }
    future.wait();
  }

  static void TearDownTestCase() {
    communicator_->stop();
    communicator_.reset();
    communicatorThread_->join();
    communicatorThread_.reset();
  }

  void SetUp() override {
    VLOG(0) << "creating pool";
    pool_ = facebook::velox::memory::memoryManager()->addLeafPool(
        "CudfTestMemoryPool");
  }

  exec::Split remoteSplit(const std::string& taskId, int partitionId) {
    std::string remoteUrl =
        "http://127.0.0.1:" + std::to_string(kCommunicatorPort - 3) +
        "/v1/task/" + taskId + "/results/" + std::to_string(partitionId);
    return exec::Split(
        std::make_shared<facebook::velox::exec::RemoteConnectorSplit>(
            remoteUrl));
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
};

INSTANTIATE_TEST_SUITE_P(
    CudfExchangeTest,
    CudfExchangeTest,
    ::testing::ValuesIn(generateTestParams()),
    ExchangeTestParamsPrinter());

TEST_P(CudfExchangeTest, basicTest) {
  VLOG(3) << "+ CudfExchangeTest::basicTest";
  ExchangeTestParams p = GetParam();
  int numUpstreamTasks = p.numUpstreamTasks;

  std::vector<std::shared_ptr<CudfPartitionedOutputMock>> sourceMocks;

  // Create n upstream tasks.
  for (int i = 0; i < numUpstreamTasks; i++) {
    const std::string srcTaskId = "sourceTask" + std::to_string(i);
    auto srcTask =
        createSourceTask(srcTaskId, pool_, CudfTestData::kTestRowType);

    // tell the queue manager that a new source task exists.
    queueManager_->initializeTask(srcTask, p.numPartitions, p.numSrcDrivers);

    sourceMocks.emplace_back(
        std::make_shared<CudfPartitionedOutputMock>(
            srcTaskId,
            p.numSrcDrivers,
            p.numPartitions,
            p.numChunks,
            p.numRowsPerChunk));
  }

  const std::string sinkTaskId = "sinkTask";
  int partitionId = 0;
  core::PlanNodeId exchangeNodeId;
  auto sinkTask = createExchangeTask(
      sinkTaskId, CudfTestData::kTestRowType, partitionId, exchangeNodeId);

  SinkDriverMock sinkDriver(sinkTask, p.numDstDrivers);

  // create n remote splits and add it to the sink driver mock.
  std::vector<facebook::velox::exec::Split> splits;
  for (int i = 0; i < numUpstreamTasks; i++) {
    const std::string srcTaskId = "sourceTask" + std::to_string(i);
    splits.emplace_back(remoteSplit(srcTaskId, partitionId));
  }
  sinkDriver.addSplits(splits);

  // Start the mocks.
  VLOG(3) << "Starting source tasks";
  for (int i = 0; i < numUpstreamTasks; i++) {
    sourceMocks[i]->run();
  }
  VLOG(3) << "Starting sink task";
  sinkDriver.run();

  for (int i = 0; i < numUpstreamTasks; i++) {
    sourceMocks[i]->joinThreads();
  }
  VLOG(3) << "Source tasks done.";
  sinkDriver.joinThreads();
  VLOG(3) << "Sink task done.";

  size_t expectedRows =
      p.numChunks * p.numRowsPerChunk * numUpstreamTasks * p.numSrcDrivers;
  size_t effectiveRows = sinkDriver.numRows();

  GTEST_ASSERT_EQ(expectedRows, effectiveRows);

  // Remove the srcTasks from the queue manager, so queue get freed
  for (int i = 0; i < numUpstreamTasks; i++) {
    const std::string srcTaskId = "sourceTask" + std::to_string(i);
    queueManager_->removeTask(srcTaskId);
  }

  VLOG(3) << "- CudfExchangeTest::basicTest";
}

TEST_P(CudfExchangeTest, dataIntegrityTest) {
  VLOG(3) << "+ CudfExchangeTest::dataIntegrityTest";
  ExchangeTestParams p = GetParam();
  int numUpstreamTasks = p.numUpstreamTasks;

  // Create some reference data to send which we will check against at the
  // receiver
  std::shared_ptr<CudfTestData> dataToSend = std::make_shared<CudfTestData>();
  dataToSend->initialize(p.numRowsPerChunk);

  std::vector<std::shared_ptr<CudfPartitionedOutputMock>> sourceMocks;

  // Create n upstream tasks.
  for (int i = 0; i < numUpstreamTasks; i++) {
    const std::string srcTaskId = "sourceTask" + std::to_string(i);
    auto srcTask =
        createSourceTask(srcTaskId, pool_, CudfTestData::kTestRowType);

    // tell the queue manager that a new source task exists.
    queueManager_->initializeTask(srcTask, p.numPartitions, p.numSrcDrivers);

    // Mock the CudfPartitionedOutput operator, it will produce numChunks of
    // data each containing numRowsPerChunk of data copied from the CudfTestData
    // object data
    sourceMocks.emplace_back(
        std::make_shared<CudfPartitionedOutputMock>(
            srcTaskId,
            p.numSrcDrivers,
            p.numPartitions,
            p.numChunks,
            p.numRowsPerChunk,
            dataToSend));
  }

  const std::string sinkTaskId = "sinkTask";
  int partitionId = 0;
  core::PlanNodeId exchangeNodeId;
  auto sinkTask = createExchangeTask(
      sinkTaskId, CudfTestData::kTestRowType, partitionId, exchangeNodeId);

  SinkDriverMock sinkDriver(sinkTask, p.numDstDrivers, dataToSend);

  // create n remote splits and add it to the sink driver mock.
  std::vector<facebook::velox::exec::Split> splits;
  for (int i = 0; i < numUpstreamTasks; i++) {
    const std::string srcTaskId = "sourceTask" + std::to_string(i);
    splits.emplace_back(remoteSplit(srcTaskId, partitionId));
  }
  sinkDriver.addSplits(splits);

  // Start the mocks.
  VLOG(3) << "Starting source tasks";
  for (int i = 0; i < numUpstreamTasks; i++) {
    sourceMocks[i]->run();
  }

  VLOG(3) << "Starting sink task";
  sinkDriver.run();

  for (int i = 0; i < numUpstreamTasks; i++) {
    sourceMocks[i]->joinThreads();
  }
  VLOG(3) << "Source tasks done.";

  sinkDriver.joinThreads();
  VLOG(3) << "Sink task done.";

  // Remove the srcTasks from the queue manager, so queue get freed
  for (int i = 0; i < numUpstreamTasks; i++) {
    const std::string srcTaskId = "sourceTask" + std::to_string(i);
    queueManager_->removeTask(srcTaskId);
  }

  VLOG(3) << "- CudfExchangeTest::dataIntegrityTest";
  GTEST_ASSERT_EQ(sinkDriver.dataIsValid(), true);
}

TEST_P(CudfExchangeTest, bandwidthTest) {
  // Test to measure the bandwidth at the Velox level

  // Skip by default, enable with environment variable
  if (!std::getenv("RUN_BANDWIDTH_TEST")) {
    GTEST_SKIP()
        << "Bandwidth test skipped. Set RUN_BANDWIDTH_TEST=1 to enable.";
  }

  VLOG(3) << "+ CudfExchangeTest::bandwidthTest";
  ExchangeTestParams p = GetParam();
  int numUpstreamTasks = p.numUpstreamTasks;

  // Create some reference data to send which we will check against at the
  // receiver
  std::shared_ptr<CudfTestData> dataToSend = std::make_shared<CudfTestData>();
  dataToSend->initialize(p.numRowsPerChunk);

  std::vector<std::shared_ptr<CudfPartitionedOutputMock>> sourceMocks;

  // Create n upstream tasks.
  for (int i = 0; i < numUpstreamTasks; i++) {
    const std::string srcTaskId = "sourceTask" + std::to_string(i);

    // Create a source task with a large maximum queue size so that we don't
    // block sending
    auto srcTask = createSourceTask(
        srcTaskId, pool_, CudfTestData::kTestRowType, FOUR_GBYTES * 10);
    queueManager_->initializeTask(srcTask, p.numPartitions, p.numSrcDrivers);

    // Mock the CudfPartitionedOutput operator, it will produce numChunks of
    // data each containing numRowsPerChunk of data copied from the CudfTestData
    // object data
    sourceMocks.emplace_back(
        std::make_shared<CudfPartitionedOutputMock>(
            srcTaskId,
            p.numSrcDrivers,
            p.numPartitions,
            p.numChunks,
            p.numRowsPerChunk,
            dataToSend));
  }

  const std::string sinkTaskId = "sinkTask";
  int partitionId = 0;
  core::PlanNodeId exchangeNodeId;
  auto sinkTask = createExchangeTask(
      sinkTaskId, CudfTestData::kTestRowType, partitionId, exchangeNodeId);

  SinkDriverMock sinkDriver(
      sinkTask, p.numDstDrivers, nullptr /* Don't check data too slow*/);

  // create n remote splits and add it to the sink driver mock.
  std::vector<facebook::velox::exec::Split> splits;
  for (int i = 0; i < numUpstreamTasks; i++) {
    const std::string srcTaskId = "sourceTask" + std::to_string(i);
    splits.emplace_back(remoteSplit(srcTaskId, partitionId));
  }
  sinkDriver.addSplits(splits);

  // Start the mocks.
  VLOG(3) << "Starting source tasks";
  for (int i = 0; i < numUpstreamTasks; i++) {
    sourceMocks[i]->run();
  }
  for (int i = 0; i < numUpstreamTasks; i++) {
    sourceMocks[i]->joinThreads();
  }
  VLOG(3) << "Source tasks done.";

  // Only starting receiving when sender is done, note this can be dangeous
  // if the total data send is larger than the queue as the source thread
  // will block and we will never arrive here

  VLOG(3) << "Starting sink task";
  std::chrono::time_point<std::chrono::high_resolution_clock> send_start =
      std::chrono::high_resolution_clock::now();

  sinkDriver.run();
  sinkDriver.joinThreads();
  std::chrono::time_point<std::chrono::high_resolution_clock> send_end =
      std::chrono::high_resolution_clock::now();

  auto rx_bytes = sinkDriver.numBytes();
  auto duration = send_end - send_start;
  auto micros =
      std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
  auto throughput = (float)rx_bytes / (float)micros;
  VLOG(3)
      << "*** duration: "
      << std::chrono::duration_cast<std::chrono::milliseconds>(duration).count()
      << " ms ";
  VLOG(3) << "*** MBytes " << (float)rx_bytes / (float)(1024 * 1024);
  VLOG(0) << "*** throughput: " << throughput << " MByte/s";

  VLOG(3) << "Sink task done.";

  // Remove the srcTasks from the queue manager, so queue get freed
  for (int i = 0; i < numUpstreamTasks; i++) {
    const std::string srcTaskId = "sourceTask" + std::to_string(i);
    queueManager_->removeTask(srcTaskId);
  }

  VLOG(3) << "- CudfExchangeTest::bandwidth";
  GTEST_ASSERT_EQ(sinkDriver.dataIsValid(), true);
}

std::shared_ptr<CudfOutputQueueManager> CudfExchangeTest::queueManager_;
std::shared_ptr<std::thread> CudfExchangeTest::communicatorThread_;
std::shared_ptr<Communicator> CudfExchangeTest::communicator_;

} // namespace facebook::velox::cudf_exchange
