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
#include "velox/common/base/Fs.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/text/RegisterTextWriter.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/serializers/CompactRowSerializer.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"

#include <folly/init/Init.h>
#include <algorithm>
#include <filesystem>
#include <iostream>
#include <string>

using namespace facebook::velox;
using namespace facebook::velox::exec;

exec::Split remoteSplit(const std::string& taskId) {
  return exec::Split(
      std::make_shared<facebook::velox::exec::RemoteConnectorSplit>(taskId));
}

// Assisted by watsonx Code Assistant

void print_file_contents(const std::filesystem::path& path) {
  if (std::filesystem::is_regular_file(path)) {
    std::ifstream file(path);
    if (file.is_open()) {
      std::string line;
      while (std::getline(file, line)) {
        std::replace(line.begin(), line.end(), '\x01', ';');
        std::cout << line << std::endl;
      }
      file.close();
    }
  } else if (std::filesystem::is_directory(path)) {
    for (const auto& entry : std::filesystem::directory_iterator(path)) {
      print_file_contents(entry.path());
    }
  }
}

// Needed by the processor task
BlockingReason
my_callback(RowVectorPtr data, bool drained, ContinueFuture* future) {
  return facebook::velox::exec::BlockingReason::kNotBlocked;
}

DEFINE_uint32(
    port,
    24356,
    "Port number (don't correct for +3)"); // Port of Remote Server
DEFINE_uint32(srv_port, 13131, "Unused communicator listening port");
DEFINE_string(hostname, "127.0.0.1", "Host name"); // Address of Remote Server
DEFINE_string(taskId, "task0", "task id"); // Task of Remote Server
DEFINE_uint32(
    destination,
    0,
    "destination"); // Partition number on Remote Server
DEFINE_int32(cuda_device, 0, "Cuda device or -1 for not setting the device");

int main(int argc, char** argv) {
  // Velox Tasks/Operators are based on folly's async framework, so we need to
  // make sure we initialize it first.

  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  setenv("UCX_TCP_CM_REUSEADDR", "y", 1);

  // Creating the CU Context, to be determined where this should be initialized
  if (FLAGS_cuda_device != -1) {
    cudaError_t err = cudaSetDevice(FLAGS_cuda_device);
    if (err !=
        cudaSuccess) { // Handle error: device might not be available, etc.
      VLOG(1) << "CUDA error: " << cudaGetErrorString(err) << std::endl;
    } else {
      VLOG(1) << "Set cuda device to " << FLAGS_cuda_device;
    }
  }

  // Default memory allocator used throughout this example.
  const memory::MemoryManagerOptions options;
  memory::MemoryManager::initialize(options);
  auto pool = memory::memoryManager()->addLeafPool();

  // To be able to read local files, we need to register the local file
  // filesystem. We also need to register the dwrf reader factory as well as a
  // write protocol, in this case commit is not required:
  filesystems::registerLocalFileSystem();
  dwio::common::registerFileSinks();
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();
  text::registerTextWriterFactory();
  // Register the presto serialized/deserializer.
  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kPresto)) {
    serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kCompactRow)) {
    facebook::velox::serializer::CompactRowVectorSerde::
        registerNamedVectorSerde();
  }

  // Register the Hive Connector Factory.
  const std::string kHiveConnectorId = "test-hive";
  connector::registerConnectorFactory(
      std::make_shared<connector::hive::HiveConnectorFactory>());
  // Create a new connector instance from the connector factory and register
  // it:
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(
              kHiveConnectorId,
              std::make_shared<config::ConfigBase>(
                  std::unordered_map<std::string, std::string>()));
  connector::registerConnector(hiveConnector);

  // Enable cuDF operators
  facebook::velox::cudf_velox::registerCudf();

  int kNumDestinations = 1;
  int kNumDrivers = 1;

  // Format for BRC
  auto selectedRowType =
      ROW({"station_name", "measurement"}, {VARCHAR(), DOUBLE()});
  // ROW({"station_name", "measurement"}, {INTEGER(), DOUBLE()});
  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  // Create a temporary dir to store the local file created. Note that this
  // directory is automatically removed when the `tempDir` object runs out of
  // scope.
  auto tempDir = exec::test::TempDirectoryPath::create();
  auto absTempDirPath = tempDir->getPath();

  LOG(INFO) << "Going to write to: " << absTempDirPath;

  facebook::velox::cudf_velox::CudfOptions::getInstance()
      .setShouldTransformLastOutput(false);
  // create a plan for processing the input read by the reader task.
  core::PlanNodeId exchangeNodeId;
  auto processorPlan =
      exec::test::PlanBuilder()
          .exchange(selectedRowType, VectorSerde::Kind::kCompactRow)
          .capturePlanNodeId(exchangeNodeId)
          .partialAggregation(
              {"station_name"},
              {"min(measurement) AS min",
               "max(measurement) AS max",
               "avg(measurement) AS avg"})
          .localPartition({})
          .finalAggregation()
          .orderBy({"station_name ASC"}, false)
          .tableWrite(absTempDirPath, dwio::common::FileFormat::TEXT)
          .planFragment();

  std::string processorTaskId = std::string("local:://processor-0"); // FIXME

  auto processorTask = exec::Task::create(
      processorTaskId,
      processorPlan,
      FLAGS_destination,
      core::QueryCtx::create(executor.get()),
      exec::Task::ExecutionMode::kParallel,
      my_callback);

  // Start communicator.
  auto communicator = cudf_exchange::Communicator::initAndGet(FLAGS_srv_port);

  // start communicator in separate thread.
  std::thread serverThread(
      &cudf_exchange::Communicator::run, communicator.get());

  // Add a remote split such that the processor task starts fetching data from
  // the reader task.

  std::string remoteUrl = "http://" + FLAGS_hostname + ":" +
      std::to_string(FLAGS_port - 3) + "/v1/task/" + FLAGS_taskId +
      "/results/" + std::to_string(FLAGS_destination);

  VLOG(3) << "Adding split: " << remoteUrl;
  processorTask->addSplit("0", remoteSplit(remoteUrl));
  processorTask->noMoreSplits("0");

  // Start the processor task with some number of drivers.
  VLOG(3) << "Starting tasks";
  processorTask->start(kNumDrivers);

  processorTask->taskCompletionFuture().wait();
  VLOG(3) << "processing task done.";

  /*std::cout << facebook::velox::exec::printPlanWithStats(
                   *processorPlan.planNode, processorTask->taskStats(), true)
            << std::endl;*/

  // cat the contents of the generated file.
  print_file_contents(absTempDirPath);

  // Clean up
  std::cout << "Going to clean-up" << std::endl;

  communicator->stop();
  serverThread.join();
  facebook::velox::cudf_velox::unregisterCudf();

  executor.reset();
  pool.reset();

  std::cout << "Going to leave main" << std::endl;
}
