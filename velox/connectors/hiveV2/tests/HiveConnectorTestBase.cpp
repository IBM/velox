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

#include "velox/connectors/hiveV2/tests/HiveConnectorTestBase.h"

#include "velox/connectors/hiveV2/HiveConnectorSplit.h"
#include "velox/connectors/hiveV2/HiveTableHandle.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"

#include <vector>

namespace facebook::velox::connector::hiveV2::test {

void HiveConnectorTestBase::SetUp() {
  hive_common::test::ConnectorTestBase::SetUp();

  connector::registerConnectorFactory(
      std::make_shared<hiveV2::HiveConnectorFactory>());
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hiveV2::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(
              kHiveConnectorId,
              std::make_shared<config::ConfigBase>(
                  std::unordered_map<std::string, std::string>()),
              ioExecutor_.get());
  connector::registerConnector(hiveConnector);
}

void HiveConnectorTestBase::TearDown() {
  ConnectorTestBase::TearDown();

  connector::unregisterConnector(kHiveConnectorId);
  connector::unregisterConnectorFactory(
      connector::hiveV2::HiveConnectorFactory::kHiveConnectorName);
}

void HiveConnectorTestBase::resetHiveConnector(
    const std::shared_ptr<const config::ConfigBase>& config) {
  connector::unregisterConnector(kHiveConnectorId);
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hiveV2::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(kHiveConnectorId, config, ioExecutor_.get());
  connector::registerConnector(hiveConnector);
}

std::shared_ptr<exec::Task> HiveConnectorTestBase::assertQuery(
    const core::PlanNodePtr& plan,
    const std::string& duckDbSql) {
  return OperatorTestBase::assertQuery(plan, duckDbSql);
}

std::shared_ptr<exec::Task> HiveConnectorTestBase::assertQuery(
    const core::PlanNodePtr& plan,
    const std::vector<std::shared_ptr<TempFilePath>>& filePaths,
    const std::string& duckDbSql) {
  return OperatorTestBase::assertQuery(
      plan, makeHiveConnectorSplits(filePaths), duckDbSql);
}

std::shared_ptr<exec::Task> HiveConnectorTestBase::assertQuery(
    const core::PlanNodePtr& plan,
    const std::vector<std::shared_ptr<ConnectorSplit>>& splits,
    const std::string& duckDbSql,
    const int32_t numPrefetchSplit) {
  return AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(
          core::QueryConfig::kMaxSplitPreloadPerDriver,
          std::to_string(numPrefetchSplit))
      .splits(splits)
      .assertResults(duckDbSql);
}

std::shared_ptr<exec::Task> HiveConnectorTestBase::assertQuery(
    const exec::CursorParameters& params,
    const std::string& duckDbSql) {
  return OperatorTestBase::assertQuery(params, duckDbSql);
}

std::vector<std::shared_ptr<connector::ConnectorSplit>>
HiveConnectorTestBase::makeHiveConnectorSplits(
    const std::vector<std::shared_ptr<TempFilePath>>& filePaths) {
  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
  for (auto filePath : filePaths) {
    splits.push_back(makeHiveConnectorSplit(
        filePath->getPath(),
        filePath->fileSize(),
        filePath->fileModifiedTime(),
        0,
        std::numeric_limits<uint64_t>::max()));
  }
  return splits;
}

std::vector<std::shared_ptr<connector::hiveV2::HiveConnectorSplit>>
HiveConnectorTestBase::makeHiveConnectorSplits(
    const std::string& filePath,
    uint32_t splitCount,
    dwio::common::FileFormat format,
    const std::optional<
        std::unordered_map<std::string, std::optional<std::string>>>&
        partitionKeys,
    const std::optional<std::unordered_map<std::string, std::string>>&
        infoColumns) {
  auto file =
      filesystems::getFileSystem(filePath, nullptr)->openFileForRead(filePath);
  const int64_t fileSize = file->size();
  // Take the upper bound.
  const int64_t splitSize = std::ceil((fileSize) / splitCount);

  std::vector<std::shared_ptr<connector::hiveV2::HiveConnectorSplit>> splits;
  // Add all the splits.
  for (int i = 0; i < splitCount; i++) {
    auto splitBuilder = HiveConnectorSplitBuilder(filePath)
                            .fileFormat(format)
                            .start(i * splitSize)
                            .length(splitSize);
    if (infoColumns.has_value()) {
      for (auto infoColumn : infoColumns.value()) {
        splitBuilder.infoColumn(infoColumn.first, infoColumn.second);
      }
    }
    if (partitionKeys.has_value()) {
      for (auto partitionKey : partitionKeys.value()) {
        splitBuilder.partitionKey(partitionKey.first, partitionKey.second);
      }
    }

    auto split = splitBuilder.build();
    splits.push_back(std::move(split));
  }

  return splits;
}

std::shared_ptr<connector::hiveV2::HiveConnectorSplit>
HiveConnectorTestBase::makeHiveConnectorSplit(
    const std::string& filePath,
    uint64_t start,
    uint64_t length,
    int64_t splitWeight,
    bool cacheable) {
  return HiveConnectorSplitBuilder(filePath)
      .start(start)
      .length(length)
      .splitWeight(splitWeight)
      .cacheable(cacheable)
      .build();
}

std::shared_ptr<connector::hiveV2::HiveConnectorSplit>
HiveConnectorTestBase::makeHiveConnectorSplit(
    const std::string& filePath,
    int64_t fileSize,
    int64_t fileModifiedTime,
    uint64_t start,
    uint64_t length) {
  return HiveConnectorSplitBuilder(filePath)
      .infoColumn("$file_size", fmt::format("{}", fileSize))
      .infoColumn("$file_modified_time", fmt::format("{}", fileModifiedTime))
      .start(start)
      .length(length)
      .build();
}

std::unique_ptr<connector::hiveV2::HiveColumnHandle>
HiveConnectorTestBase::makeHiveColumnHandle(
    const std::string& name,
    const TypePtr& type,
    const std::vector<std::string>& requiredSubfields) {
  return makeHiveColumnHandle(name, type, type, requiredSubfields);
}

std::unique_ptr<connector::hiveV2::HiveColumnHandle>
HiveConnectorTestBase::makeHiveColumnHandle(
    const std::string& name,
    const TypePtr& dataType,
    const TypePtr& hiveType,
    const std::vector<std::string>& requiredSubfields,
    connector::hiveV2::HiveColumnHandle::ColumnType columnType) {
  std::vector<common::Subfield> subfields;
  subfields.reserve(requiredSubfields.size());
  for (auto& path : requiredSubfields) {
    subfields.emplace_back(path);
  }

  return std::make_unique<connector::hiveV2::HiveColumnHandle>(
      name, columnType, dataType, hiveType, std::move(subfields));
}

// static
std::shared_ptr<connector::hiveV2::HiveInsertTableHandle>
HiveConnectorTestBase::makeHiveInsertTableHandle(
    const std::vector<std::string>& tableColumnNames,
    const std::vector<TypePtr>& tableColumnTypes,
    const std::vector<std::string>& partitionedBy,
    std::shared_ptr<connector::hiveV2::LocationHandle> locationHandle,
    const dwio::common::FileFormat tableStorageFormat,
    const std::optional<common::CompressionKind> compressionKind,
    const std::shared_ptr<dwio::common::WriterOptions>& writerOptions,
    const bool ensureFiles) {
  return makeHiveInsertTableHandle(
      tableColumnNames,
      tableColumnTypes,
      partitionedBy,
      nullptr,
      std::move(locationHandle),
      tableStorageFormat,
      compressionKind,
      {},
      writerOptions,
      ensureFiles);
}

// static
std::shared_ptr<connector::hiveV2::HiveInsertTableHandle>
HiveConnectorTestBase::makeHiveInsertTableHandle(
    const std::vector<std::string>& tableColumnNames,
    const std::vector<TypePtr>& tableColumnTypes,
    const std::vector<std::string>& partitionedBy,
    std::shared_ptr<connector::hiveV2::HiveBucketProperty> bucketProperty,
    std::shared_ptr<connector::hiveV2::LocationHandle> locationHandle,
    const dwio::common::FileFormat tableStorageFormat,
    const std::optional<common::CompressionKind> compressionKind,
    const std::unordered_map<std::string, std::string>& serdeParameters,
    const std::shared_ptr<dwio::common::WriterOptions>& writerOptions,
    const bool ensureFiles) {
  std::vector<std::shared_ptr<const connector::hiveV2::HiveColumnHandle>>
      columnHandles;
  std::vector<std::string> bucketedBy;
  std::vector<TypePtr> bucketedTypes;
  std::vector<std::shared_ptr<const connector::hiveV2::HiveSortingColumn>>
      sortedBy;
  if (bucketProperty != nullptr) {
    bucketedBy = bucketProperty->bucketedBy();
    bucketedTypes = bucketProperty->bucketedTypes();
    sortedBy = bucketProperty->sortedBy();
  }
  int32_t numPartitionColumns{0};
  int32_t numSortingColumns{0};
  int32_t numBucketColumns{0};
  for (int i = 0; i < tableColumnNames.size(); ++i) {
    for (int j = 0; j < bucketedBy.size(); ++j) {
      if (bucketedBy[j] == tableColumnNames[i]) {
        ++numBucketColumns;
      }
    }
    for (int j = 0; j < sortedBy.size(); ++j) {
      if (sortedBy[j]->sortColumn() == tableColumnNames[i]) {
        ++numSortingColumns;
      }
    }
    if (std::find(
            partitionedBy.cbegin(),
            partitionedBy.cend(),
            tableColumnNames.at(i)) != partitionedBy.cend()) {
      ++numPartitionColumns;
      columnHandles.push_back(
          std::make_shared<connector::hiveV2::HiveColumnHandle>(
              tableColumnNames.at(i),
              connector::hiveV2::HiveColumnHandle::ColumnType::kPartitionKey,
              tableColumnTypes.at(i),
              tableColumnTypes.at(i)));
    } else {
      columnHandles.push_back(
          std::make_shared<connector::hiveV2::HiveColumnHandle>(
              tableColumnNames.at(i),
              connector::hiveV2::HiveColumnHandle::ColumnType::kRegular,
              tableColumnTypes.at(i),
              tableColumnTypes.at(i)));
    }
  }
  VELOX_CHECK_EQ(numPartitionColumns, partitionedBy.size());
  VELOX_CHECK_EQ(numBucketColumns, bucketedBy.size());
  VELOX_CHECK_EQ(numSortingColumns, sortedBy.size());

  return std::make_shared<connector::hiveV2::HiveInsertTableHandle>(
      columnHandles,
      locationHandle,
      tableStorageFormat,
      bucketProperty,
      compressionKind,
      serdeParameters,
      writerOptions,
      ensureFiles);
}

std::shared_ptr<connector::hiveV2::HiveColumnHandle>
HiveConnectorTestBase::regularHiveColumn(
    const std::string& name,
    const TypePtr& type) {
  return std::make_shared<connector::hiveV2::HiveColumnHandle>(
      name,
      connector::hiveV2::HiveColumnHandle::ColumnType::kRegular,
      type,
      type);
}

std::shared_ptr<connector::hiveV2::HiveColumnHandle>
HiveConnectorTestBase::synthesizedHiveColumn(
    const std::string& name,
    const TypePtr& type) {
  return std::make_shared<connector::hiveV2::HiveColumnHandle>(
      name,
      connector::hiveV2::HiveColumnHandle::ColumnType::kSynthesized,
      type,
      type);
}

std::shared_ptr<connector::hiveV2::HiveColumnHandle>
HiveConnectorTestBase::hivePartitionKey(
    const std::string& name,
    const TypePtr& type) {
  return std::make_shared<connector::hiveV2::HiveColumnHandle>(
      name,
      connector::hiveV2::HiveColumnHandle::ColumnType::kPartitionKey,
      type,
      type);
}

} // namespace facebook::velox::connector::hiveV2::test
