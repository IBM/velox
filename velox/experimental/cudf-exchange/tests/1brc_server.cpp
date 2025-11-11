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
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/text/RegisterTextWriter.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/experimental/cudf-exchange/Communicator.h"
#include "velox/experimental/cudf-exchange/CudfOutputQueueManager.h"
#include "velox/experimental/cudf/connectors/parquet/ParquetConnector.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/serializers/CompactRowSerializer.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"

#include <cuda_runtime.h>
#include <folly/init/Init.h>
#include <algorithm>
#include <filesystem>
#include <iostream>
#include <string>

using namespace facebook::velox;
using namespace facebook::velox::exec;

DEFINE_string(inputfile, "measurements-100.parquet ", "input parquet file");
DEFINE_uint32(
    port,
    24356 + 3,
    "Port number"); // "+3" accounts for the hack for Presto ! See
                    // cudf-exchange/CudfExchangeSource.cpp
DEFINE_string(taskId, "task0", "task id");
DEFINE_uint32(cudfChunkSizeGB, 1, "cuDF Parquet chunk size to read in GB");
DEFINE_int32(cuda_device, 0, "Cuda device or -1 for not setting the device");
DEFINE_bool(use_hive, false, "Use Hive Connector");

int main(int argc, char** argv) {
  // Velox Tasks/Operators are based on folly's async framework, so we need to
  // make sure we initialize it first.

  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  setenv("UCX_TCP_CM_REUSEADDR", "y", 1);

  if (FLAGS_cuda_device != -1) {
    cudaError_t err = cudaSetDevice(FLAGS_cuda_device); // Different than Server
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

  // In order to read and write data and files from storage, we need to use a
  // Connector. Let's instantiate and register a HiveConnector for this
  // example:

  // We need a connector id string to identify the connector.
  const std::string kHiveConnectorId = "test-hive";

  // Register the Hive Connector Factory.
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
  parquet::registerParquetReaderFactory();

  // To be able to read local files, we need to register the local file
  // filesystem. We also need to register the dwrf reader factory as well as a
  // write protocol, in this case commit is not required:

  filesystems::registerLocalFileSystem();
  // dwio::common::registerFileSinks();
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();
  // text::registerTextWriterFactory();
  //  The following registers a LocalExchangeSource that directly taps into the
  //  node's OutputBufferManager to request pages for the given destination.
  exec::ExchangeSource::registerFactory(
      facebook::velox::exec::test::createLocalExchangeSource);
  // Register the presto serialized/deserializer.
  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kPresto)) {
    serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kCompactRow)) {
    facebook::velox::serializer::CompactRowVectorSerde::
        registerNamedVectorSerde();
  }

  // Register the CUDF Parquet reader
  // It notices its existence with a hardcoded name "test-parquet"
  // set NOT means it will not be recognized
  const char* kCudfParquetConnectorName = "test-parquet";
  if (FLAGS_use_hive) {
    kCudfParquetConnectorName = "NOT-test-parquet";
    facebook::velox::cudf_velox::CudfOptions::getInstance()
        .setParquetConnectorRegistered(false);

  } else {
    facebook::velox::cudf_velox::CudfOptions::getInstance()
        .setParquetConnectorRegistered(true);
  }
  if (!facebook::velox::connector::hasConnectorFactory(
          kCudfParquetConnectorName)) {
    facebook::velox::connector::registerConnectorFactory(
        std::make_shared<facebook::velox::cudf_velox::connector::parquet::
                             ParquetConnectorFactory>(
            kCudfParquetConnectorName));
    // This is how ToCudf recognizes that we are using the
    // cudf-based parquet reader that produces Cudf vectors.
  }

  std::unordered_map<std::string, std::string> c = {};
  LOG(INFO) << "reading " << FLAGS_cudfChunkSizeGB << "GB chunks at once";
  c[facebook::velox::cudf_velox::connector::parquet::ParquetConfig::
        kMaxChunkReadLimit] =
      std::to_string(FLAGS_cudfChunkSizeGB * 1024 * 1024 * 1024);
  std::shared_ptr<const facebook::velox::config::ConfigBase> properties =
      std::make_shared<const facebook::velox::config::ConfigBase>(std::move(c));

  std::shared_ptr<facebook::velox::connector::Connector> connector =
      facebook::velox::connector::getConnectorFactory(kCudfParquetConnectorName)
          ->newConnector(kCudfParquetConnectorName, std::move(properties));
  facebook::velox::connector::registerConnector(connector);

  // Enable cuDF operators
  facebook::velox::cudf_velox::registerCudf();

  int kNumDestinations = 1;
  int kNumDrivers = 1;
  // Define a query plan that reads data from parquet.
  core::PlanNodeId scanNodeId;
  core::PlanNodeId partitionNodeId;

  auto selectedRowType =
      ROW({"station_name", "measurement"}, {VARCHAR(), DOUBLE()});

  auto readerPlan = exec::test::PlanBuilder()
                        .tableScan(asRowType(selectedRowType))
                        .capturePlanNodeId(scanNodeId)
                        .project({"station_name", "measurement"})
                        .partitionedOutput(
                            {}, // No partitioning key.
                            kNumDestinations, // just one destination.
                            std::vector<std::string>{
                                "station_name", "measurement"}, // output layout
                            VectorSerde::Kind::kCompactRow)
                        .planFragment();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  std::unordered_map<std::string, std::string> configSettings;
  auto queryCtx = core::QueryCtx::create(
      executor.get(), core::QueryConfig(std::move(configSettings)));

  // create the reader task.
  std::string readerTaskId = std::string(FLAGS_taskId);
  auto readerTask = exec::Task::create(
      readerTaskId,
      readerPlan,
      /*destination=*/0,
      core::QueryCtx::create(executor.get()),
      exec::Task::ExecutionMode::kParallel);

  // Now that we have the query fragment and Task structure set up, we will
  // add data to it via `splits`.
  //
  // To pump data through a HiveConnector, we need to create a
  // HiveConnectorSplit for each file, using the same HiveConnector id defined
  // above, the local file path (the "file:" prefix specifies which FileSystem
  // to use; local, in this case), and the file format (PARQUET).

  auto filePath = "file:" + std::filesystem::path(FLAGS_inputfile).string();
  VLOG(3) << "Reading parquet file: " << filePath;

  std::shared_ptr<facebook::velox::connector::ConnectorSplit> connectorSplit;

  if (FLAGS_use_hive) {
    connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
        kHiveConnectorId, filePath, dwio::common::FileFormat::PARQUET);
  } else {
    connectorSplit = std::make_shared<
        facebook::velox::cudf_velox::connector::parquet::ParquetConnectorSplit>(
        "test-parquet", std::filesystem::path(FLAGS_inputfile).string(), 0);
  }

  // Wrap it in a `Split` object and add to the task. We need to specify to
  // which operator we're adding the split (that's why we captured the
  // TableScan's id above). Here we could pump subsequent split/files into
  // the TableScan.
  readerTask->addSplit(scanNodeId, exec::Split{std::move(connectorSplit)});

  // Signal that no more splits will be added. After this point, calling
  // next() on the task will start the readerPlan execution using the current
  // thread.
  readerTask->noMoreSplits(scanNodeId);

  auto communicator = cudf_exchange::Communicator::initAndGet(FLAGS_port);

  // start communicator in separate thread.
  std::thread serverThread(
      &cudf_exchange::Communicator::run, communicator.get());

  // Start the processor task with some number of drivers.
  VLOG(3) << "Starting Reader Task";
  readerTask->start(kNumDrivers);
  readerTask->taskCompletionFuture().wait();
  VLOG(3) << "reader task done.";

  VLOG(3) << printPlanWithStats(
      *readerPlan.planNode, readerTask->taskStats(), true);

  communicator->stop();
  serverThread.join();

  // Clean up
  facebook::velox::cudf_velox::unregisterCudf();

  pool.reset();

  executor.reset();
}
