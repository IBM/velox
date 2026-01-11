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

#include "ConnectorTestBase.h"

#include "velox/common/file/tests/FaultyFileSystem.h"
#include "velox/connectors/lakehouse/iceberg/ConnectorSplitBase.h"
#include "velox/connectors/lakehouse/iceberg/IcebergConnector.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/dwio/dwrf/writer/Writer.h"

using namespace facebook::velox::connector::lakehouse::iceberg;

namespace facebook::velox::connector::lakehouse::iceberg::test {

using namespace facebook::velox;
using namespace facebook::velox::common;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

ConnectorTestBase::ConnectorTestBase() {
  filesystems::registerLocalFileSystem();
  velox::tests::utils::registerFaultyFileSystem();
}

void ConnectorTestBase::SetUp() {
  OperatorTestBase::SetUp();
  auto icebergConnector =
      std::make_shared<iceberg::IcebergConnector>(kIcebergConnectorId,
                                                  std::make_shared<config::ConfigBase>(
                                                      std::unordered_map<std::string, std::string>()),
                                                  ioExecutor_.get());
    connector::registerConnector(icebergConnector);
  dwio::common::registerFileSinks();
  dwrf::registerDwrfReaderFactory();
  dwrf::registerDwrfWriterFactory();
}

void ConnectorTestBase::TearDown() {
  // Make sure all pending loads are finished or cancelled before unregister
  // connector.
  ioExecutor_.reset();
  dwrf::unregisterDwrfReaderFactory();
  dwrf::unregisterDwrfWriterFactory();
  connector::unregisterConnector(kIcebergConnectorId);
  connector::unregisterConnector(
      iceberg::IcebergConnectorFactory::kIcebergConnectorName);
  OperatorTestBase::TearDown();
}

 void ConnectorTestBase::resetIcebergConnector(
     const std::shared_ptr<const config::ConfigBase>& config) {
   connector::unregisterConnector(kIcebergConnectorId);
   auto icebergConnector =
       std::make_shared<iceberg::IcebergConnector>(kIcebergConnectorId, config, ioExecutor_.get());
   connector::registerConnector(icebergConnector);
 }

void ConnectorTestBase::writeToFiles(
    const std::vector<std::string>& filePaths,
    std::vector<RowVectorPtr> vectors) {
  VELOX_CHECK_EQ(filePaths.size(), vectors.size());
  for (int i = 0; i < filePaths.size(); ++i) {
    writeToFile(filePaths[i], std::vector{vectors[i]});
  }
}

void ConnectorTestBase::writeToFile(
    const std::string& filePath,
    RowVectorPtr vector) {
  writeToFile(filePath, std::vector{vector});
}

void ConnectorTestBase::writeToFile(
    const std::string& filePath,
    const std::vector<RowVectorPtr>& vectors,
    std::shared_ptr<dwrf::Config> config,
    const std::function<std::unique_ptr<dwrf::DWRFFlushPolicy>()>&
        flushPolicyFactory) {
  writeToFile(
      filePath,
      vectors,
      std::move(config),
      vectors[0]->type(),
      flushPolicyFactory);
}

void ConnectorTestBase::writeToFile(
    const std::string& filePath,
    const std::vector<RowVectorPtr>& vectors,
    std::shared_ptr<dwrf::Config> config,
    const TypePtr& schema,
    const std::function<std::unique_ptr<dwrf::DWRFFlushPolicy>()>&
        flushPolicyFactory) {
  velox::dwrf::WriterOptions options;
  options.config = config;
  options.schema = schema;
  auto fs = filesystems::getFileSystem(filePath, {});
  auto writeFile = fs->openFileForWrite(
      filePath,
      {.shouldCreateParentDirectories = true,
       .shouldThrowOnFileAlreadyExists = false});
  auto sink = std::make_unique<dwio::common::WriteFileSink>(
      std::move(writeFile), filePath);
  auto childPool = rootPool_->addAggregateChild("ConnectorTestBase.Writer");
  options.memoryPool = childPool.get();
  options.flushPolicyFactory = flushPolicyFactory;

  facebook::velox::dwrf::Writer writer{std::move(sink), options};
  for (size_t i = 0; i < vectors.size(); ++i) {
    writer.write(vectors[i]);
  }
  writer.close();
}

void ConnectorTestBase::createDirectory(const std::string& directoryPath) {
  auto fs = filesystems::getFileSystem(directoryPath, {});
  fs->mkdir(directoryPath);
}

void ConnectorTestBase::removeDirectory(const std::string& directoryPath) {
  auto fs = filesystems::getFileSystem(directoryPath, {});
  if (fs->exists(directoryPath)) {
    fs->rmdir(directoryPath);
  }
}

void ConnectorTestBase::removeFile(const std::string& filePath) {
  auto fs = filesystems::getFileSystem(filePath, {});
  if (fs->exists(filePath)) {
    fs->remove(filePath);
  }
}

std::vector<RowVectorPtr> ConnectorTestBase::makeVectors(
    const RowTypePtr& rowType,
    int32_t numVectors,
    int32_t rowsPerVector) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < numVectors; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        velox::test::BatchMaker::createBatch(rowType, rowsPerVector, *pool_));
    vectors.push_back(vector);
  }
  return vectors;
}

std::vector<std::shared_ptr<TempFilePath>> ConnectorTestBase::makeFilePaths(
    int count) {
  std::vector<std::shared_ptr<TempFilePath>> filePaths;

  filePaths.reserve(count);
  for (auto i = 0; i < count; ++i) {
    filePaths.emplace_back(TempFilePath::create());
  }
  return filePaths;
}

std::shared_ptr<ColumnHandleBase> ConnectorTestBase::partitionKey(
    const std::string& name,
    const TypePtr& type) {
  return std::make_shared<ColumnHandleBase>(
      name,
      ColumnHandleBase::ColumnType::kPartitionKey,
      //      type,
      type);
}

} // namespace facebook::velox::connector::lakehouse::iceberg::test
