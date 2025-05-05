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

#include "velox/connectors/hive_common/tests/ConnectorTestBase.h"

#include "velox/common/file/FileSystems.h"
#include "velox/common/file/tests/FaultyFileSystem.h"
#include "velox/connectors/hive_common/ConnectorSplitBase.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/writer/FlushPolicy.h"
#include "velox/dwio/dwrf/writer/Writer.h"
//#include "velox/exec/tests/utils/AssertQueryBuilder.h"

namespace facebook::velox::connector::hive_common::test {

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
  //  connector::registerConnectorFactory(
  //      std::make_shared<hive::HiveConnectorFactory>());
  //  auto hiveConnector =
  //      connector::getConnectorFactory(
  //          connector::hiveV2::HiveConnectorFactory::kHiveConnectorName)
  //          ->newConnector(
  //              kHiveConnectorId,
  //              std::make_shared<config::ConfigBase>(
  //                  std::unordered_map<std::string, std::string>()),
  //              ioExecutor_.get());
  //  connector::registerConnector(hiveConnector);
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
  //  connector::unregisterConnector(kHiveConnectorId);
  //  connector::unregisterConnectorFactory(
  //      connector::hiveV2::HiveConnectorFactory::kHiveConnectorName);
  OperatorTestBase::TearDown();
}

// void ConnectorTestBase::resetHiveConnector(
//     const std::shared_ptr<const config::ConfigBase>& config) {
//   connector::unregisterConnector(kHiveConnectorId);
//   auto hiveConnector =
//       connector::getConnectorFactory(
//           connector::hiveV2::HiveConnectorFactory::kHiveConnectorName)
//           ->newConnector(kHiveConnectorId, config, ioExecutor_.get());
//   connector::registerConnector(hiveConnector);
// }

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

//std::shared_ptr<exec::Task> ConnectorTestBase::assertQuery(
//    const core::PlanNodePtr& plan,
//    const std::vector<std::shared_ptr<TempFilePath>>& filePaths,
//    const std::string& duckDbSql) {
//  return OperatorTestBase::assertQuery(
//      plan, makeConnectorSplits(filePaths), duckDbSql);
//}

//std::shared_ptr<Task> ConnectorTestBase::assertQuery(
//    const core::PlanNodePtr& plan,
//    const std::vector<std::shared_ptr<ConnectorSplit>>& splits,
//    const std::string& duckDbSql,
//    const int32_t numPrefetchSplit) {
//  return AssertQueryBuilder(plan, duckDbQueryRunner_)
//      .config(
//          core::QueryConfig::kMaxSplitPreloadPerDriver,
//          std::to_string(numPrefetchSplit))
//      .splits(splits)
//      .assertResults(duckDbSql);
//}

std::vector<std::shared_ptr<TempFilePath>> ConnectorTestBase::makeFilePaths(
    int count) {
  std::vector<std::shared_ptr<TempFilePath>> filePaths;

  filePaths.reserve(count);
  for (auto i = 0; i < count; ++i) {
    filePaths.emplace_back(TempFilePath::create());
  }
  return filePaths;
}
//
//std::vector<std::shared_ptr<connector::ConnectorSplit>>
//ConnectorTestBase::makeConnectorSplits(
//    const std::vector<std::shared_ptr<TempFilePath>>& filePaths) {
//  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
//  for (auto filePath : filePaths) {
//    splits.push_back(makeConnectorSplit(
//        filePath->getPath(),
//        filePath->fileSize(),
//        filePath->fileModifiedTime(),
//        0,
//        std::numeric_limits<uint64_t>::max()));
//  }
//  return splits;
//}
//
//std::shared_ptr<connector::ConnectorSplit>
//ConnectorTestBase::makeConnectorSplit(
//    const std::string& filePath,
//    uint64_t start,
//    uint64_t length,
//    int64_t splitWeight,
//    bool cacheable) {
//  return ConnectorSplitBuilder(filePath)
//      .start(start)
//      .length(length)
//      .splitWeight(splitWeight)
//      .cacheable(cacheable)
//      .build();
//}
//
//std::shared_ptr<connector::ConnectorSplit>
//ConnectorTestBase::makeConnectorSplit(
//    const std::string& filePath,
//    int64_t fileSize,
//    int64_t fileModifiedTime,
//    uint64_t start,
//    uint64_t length) {
//  return ConnectorSplitBuilder(filePath)
//      .infoColumn("$file_size", fmt::format("{}", fileSize))
//      .infoColumn("$file_modified_time", fmt::format("{}", fileModifiedTime))
//      .start(start)
//      .length(length)
//      .build();
//}
//
//std::vector<std::shared_ptr<connector::ConnectorSplit>>
//ConnectorTestBase::makeConnectorSplits(
//    const std::string& filePath,
//    uint32_t splitCount,
//    dwio::common::FileFormat format,
//    const std::optional<
//        std::unordered_map<std::string, std::optional<std::string>>>&
//        partitionKeys,
//    const std::optional<std::unordered_map<std::string, std::string>>&
//        infoColumns) {
//  auto file =
//      filesystems::getFileSystem(filePath, nullptr)->openFileForRead(filePath);
//  const int64_t fileSize = file->size();
//  // Take the upper bound.
//  const int64_t splitSize = std::ceil((fileSize) / splitCount);
//  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
//  // Add all the splits.
//  for (int i = 0; i < splitCount; i++) {
//    auto splitBuilder = hive_common::ConnectorSplitBuilder(filePath)
//                            .fileFormat(format)
//                            .start(i * splitSize)
//                            .length(splitSize);
//    if (infoColumns.has_value()) {
//      for (auto infoColumn : infoColumns.value()) {
//        splitBuilder.infoColumn(infoColumn.first, infoColumn.second);
//      }
//    }
//    if (partitionKeys.has_value()) {
//      for (auto partitionKey : partitionKeys.value()) {
//        splitBuilder.partitionKey(partitionKey.first, partitionKey.second);
//      }
//    }
//
//    auto split = splitBuilder.build();
//    splits.push_back(std::move(split));
//  }
//  return splits;
//}

//
//std::unique_ptr<hive_common::ColumnHandleBase>
//ConnectorTestBase::makeColumnHandle(
//    const std::string& name,
//    const TypePtr& type,
//    const std::vector<std::string>& requiredSubfields) {
//  return makeColumnHandle(name, type, type, requiredSubfields);
//}

//
//// static
// std::shared_ptr<hive::HiveInsertTableHandle>
// ConnectorTestBase::makeHiveInsertTableHandle(
//     const std::vector<std::string>& tableColumnNames,
//     const std::vector<TypePtr>& tableColumnTypes,
//     const std::vector<std::string>& partitionedBy,
//     std::shared_ptr<hive::LocationHandle> locationHandle,
//     const dwio::common::FileFormat tableStorageFormat,
//     const std::optional<common::CompressionKind> compressionKind,
//     const std::shared_ptr<dwio::common::WriterOptions>& writerOptions,
//     const bool ensureFiles) {
//   return makeHiveInsertTableHandle(
//       tableColumnNames,
//       tableColumnTypes,
//       partitionedBy,
//       nullptr,
//       std::move(locationHandle),
//       tableStorageFormat,
//       compressionKind,
//       {},
//       writerOptions,
//       ensureFiles);
// }
//
//// static
// std::shared_ptr<hive::HiveInsertTableHandle>
// ConnectorTestBase::makeHiveInsertTableHandle(
//     const std::vector<std::string>& tableColumnNames,
//     const std::vector<TypePtr>& tableColumnTypes,
//     const std::vector<std::string>& partitionedBy,
//     std::shared_ptr<hive::HiveBucketProperty> bucketProperty,
//     std::shared_ptr<hive::LocationHandle> locationHandle,
//     const dwio::common::FileFormat tableStorageFormat,
//     const std::optional<common::CompressionKind> compressionKind,
//     const std::unordered_map<std::string, std::string>& serdeParameters,
//     const std::shared_ptr<dwio::common::WriterOptions>& writerOptions,
//     const bool ensureFiles) {
//   std::vector<std::shared_ptr<const
//   connector::hive_common::ColumnHandleBase>>
//       columnHandles;
//   std::vector<std::string> bucketedBy;
//   std::vector<TypePtr> bucketedTypes;
//   std::vector<std::shared_ptr<const connector::hiveV2::HiveSortingColumn>>
//       sortedBy;
//   if (bucketProperty != nullptr) {
//     bucketedBy = bucketProperty->bucketedBy();
//     bucketedTypes = bucketProperty->bucketedTypes();
//     sortedBy = bucketProperty->sortedBy();
//   }
//   int32_t numPartitionColumns{0};
//   int32_t numSortingColumns{0};
//   int32_t numBucketColumns{0};
//   for (int i = 0; i < tableColumnNames.size(); ++i) {
//     for (int j = 0; j < bucketedBy.size(); ++j) {
//       if (bucketedBy[j] == tableColumnNames[i]) {
//         ++numBucketColumns;
//       }
//     }
//     for (int j = 0; j < sortedBy.size(); ++j) {
//       if (sortedBy[j]->sortColumn() == tableColumnNames[i]) {
//         ++numSortingColumns;
//       }
//     }
//     if (std::find(
//             partitionedBy.cbegin(),
//             partitionedBy.cend(),
//             tableColumnNames.at(i)) != partitionedBy.cend()) {
//       ++numPartitionColumns;
//       columnHandles.push_back(
//           std::make_shared<hive_common::ColumnHandleBase>(
//               tableColumnNames.at(i),
//               connector::hive_common::ColumnHandleBase::ColumnType::kPartitionKey,
//               tableColumnTypes.at(i),
//               tableColumnTypes.at(i)));
//     } else {
//       columnHandles.push_back(
//           std::make_shared<hive_common::ColumnHandleBase>(
//               tableColumnNames.at(i),
//               connector::hive_common::ColumnHandleBase::ColumnType::kRegular,
//               tableColumnTypes.at(i),
//               tableColumnTypes.at(i)));
//     }
//   }
//   VELOX_CHECK_EQ(numPartitionColumns, partitionedBy.size());
//   VELOX_CHECK_EQ(numBucketColumns, bucketedBy.size());
//   VELOX_CHECK_EQ(numSortingColumns, sortedBy.size());
//
//   return std::make_shared<hive::HiveInsertTableHandle>(
//       columnHandles,
//       locationHandle,
//       tableStorageFormat,
//       bucketProperty,
//       compressionKind,
//       serdeParameters,
//       writerOptions,
//       ensureFiles);
// }

//std::shared_ptr<hive_common::ColumnHandleBase> ConnectorTestBase::regularColumn(
//    const std::string& name,
//    const TypePtr& type) {
//  return std::make_shared<hive_common::ColumnHandleBase>(
//      name, hive_common::ColumnHandleBase::ColumnType::kRegular, type);
//}

//std::shared_ptr<hive_common::ColumnHandleBase>
//ConnectorTestBase::synthesizedColumn(
//    const std::string& name,
//    const TypePtr& type) {
//  return std::make_shared<hive_common::ColumnHandleBase>(
//      name,
//      connector::hive_common::ColumnHandleBase::ColumnType::kSynthesized,
//      //      type,
//      type);
//}

std::shared_ptr<hive_common::ColumnHandleBase> ConnectorTestBase::partitionKey(
    const std::string& name,
    const TypePtr& type) {
  return std::make_shared<hive_common::ColumnHandleBase>(
      name,
      connector::hive_common::ColumnHandleBase::ColumnType::kPartitionKey,
      //      type,
      type);
}

} // namespace facebook::velox::connector::hive_common::test
