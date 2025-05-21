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
#pragma once

#include "velox/connectors/hiveV2/HiveConnector.h"
#include "velox/connectors/hiveV2/HiveConnectorSplit.h"
#include "velox/connectors/hiveV2/HiveDataSink.h"
#include "velox/connectors/hive_common/tests/ConnectorTestBase.h"
#include "velox/exec/Cursor.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/TempFilePath.h"

#include <string>

namespace facebook::velox::connector::hiveV2::test {

using namespace facebook::velox::exec::test;

static const std::string kHiveConnectorId = "test-hive";

using ColumnHandleMap =
    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>;

class HiveConnectorTestBase : public hive_common::test::ConnectorTestBase {
 public:
  HiveConnectorTestBase() {}

  void SetUp() override;
  void TearDown() override;

  void resetHiveConnector(
      const std::shared_ptr<const config::ConfigBase>& config);

  std::shared_ptr<exec::Task> assertQuery(
      const core::PlanNodePtr& plan,
      const std::string& duckDbSql);

  std::shared_ptr<exec::Task> assertQuery(
      const core::PlanNodePtr& plan,
      const std::vector<std::shared_ptr<TempFilePath>>& filePaths,
      const std::string& duckDbSql);

  std::shared_ptr<exec::Task> assertQuery(
      const core::PlanNodePtr& plan,
      const std::vector<std::shared_ptr<connector::ConnectorSplit>>& splits,
      const std::string& duckDbSql,
      const int32_t numPrefetchSplit = 0);

  std::shared_ptr<exec::Task> assertQuery(
      const exec::CursorParameters& params,
      const std::string& duckDbSql);

  static std::vector<std::shared_ptr<connector::ConnectorSplit>>
  makeHiveConnectorSplits(
      const std::vector<std::shared_ptr<TempFilePath>>& filePaths);

  /// Split file at path 'filePath' into 'splitCount' splits. If not local file,
  /// file size can be given as 'externalSize'.
  static std::vector<std::shared_ptr<connector::hiveV2::HiveConnectorSplit>>
  makeHiveConnectorSplits(
      const std::string& filePath,
      uint32_t splitCount,
      dwio::common::FileFormat format,
      const std::optional<
          std::unordered_map<std::string, std::optional<std::string>>>&
          partitionKeys = {},
      const std::optional<std::unordered_map<std::string, std::string>>&
          infoColumns = {});

  static std::shared_ptr<connector::hiveV2::HiveConnectorSplit>
  makeHiveConnectorSplit(
      const std::string& filePath,
      uint64_t start = 0,
      uint64_t length = std::numeric_limits<uint64_t>::max(),
      int64_t splitWeight = 0,
      bool cacheable = true);

  static std::shared_ptr<connector::hiveV2::HiveConnectorSplit>
  makeHiveConnectorSplit(
      const std::string& filePath,
      int64_t fileSize,
      int64_t fileModifiedTime,
      uint64_t start,
      uint64_t length);

  static std::shared_ptr<connector::hiveV2::HiveTableHandle>
  makeHiveTableHandle(
      common::SubfieldFilters subfieldFilters = {},
      const core::TypedExprPtr& remainingFilter = nullptr,
      const std::string& tableName = "hive_table",
      const RowTypePtr& dataColumns = nullptr,
      bool filterPushdownEnabled = true,
      const std::unordered_map<std::string, std::string>& tableParameters =
          {}) {
    return std::make_shared<connector::hiveV2::HiveTableHandle>(
        kHiveConnectorId,
        tableName,
        filterPushdownEnabled,
        std::move(subfieldFilters),
        remainingFilter,
        dataColumns,
        tableParameters);
  }

  /// @param name Column name.
  /// @param type Column type.
  /// @param Required subfields of this column.
  static std::unique_ptr<connector::hiveV2::HiveColumnHandle>
  makeHiveColumnHandle(
      const std::string& name,
      const TypePtr& type,
      const std::vector<std::string>& requiredSubfields);

  /// @param name Column name.
  /// @param type Column type.
  /// @param type Hive type.
  /// @param Required subfields of this column.
  static std::unique_ptr<connector::hiveV2::HiveColumnHandle>
  makeHiveColumnHandle(
      const std::string& name,
      const TypePtr& dataType,
      const TypePtr& hiveType,
      const std::vector<std::string>& requiredSubfields,
      connector::hiveV2::HiveColumnHandle::ColumnType columnType =
          connector::hiveV2::HiveColumnHandle::ColumnType::kRegular);

  /// @param targetDirectory Final directory of the target table after commit.
  /// @param writeDirectory Write directory of the target table before commit.
  /// @param tableType Whether to create a new table, insert into an existing
  /// table, or write a temporary table.
  /// @param writeMode How to write to the target directory.
  static std::shared_ptr<connector::hiveV2::LocationHandle>
  makeHiveLocationHandle(
      std::string targetDirectory,
      std::optional<std::string> writeDirectory = std::nullopt,
      connector::hiveV2::LocationHandle::TableType tableType =
          connector::hiveV2::LocationHandle::TableType::kNew) {
    return std::make_shared<connector::hiveV2::LocationHandle>(
        targetDirectory, writeDirectory.value_or(targetDirectory), tableType);
  }

  /// Build a HiveInsertTableHandle.
  /// @param tableColumnNames Column names of the target table. Corresponding
  /// type of tableColumnNames[i] is tableColumnTypes[i].
  /// @param tableColumnTypes Column types of the target table. Corresponding
  /// name of tableColumnTypes[i] is tableColumnNames[i].
  /// @param partitionedBy A list of partition columns of the target table.
  /// @param bucketProperty if not nulll, specifies the property for a bucket
  /// table.
  /// @param locationHandle Location handle for the table write.
  /// @param compressionKind compression algorithm to use for table write.
  /// @param serdeParameters Table writer configuration parameters.
  /// @param ensureFiles When this option is set the HiveDataSink will always
  /// create a file even if there is no data.
  static std::shared_ptr<connector::hiveV2::HiveInsertTableHandle>
  makeHiveInsertTableHandle(
      const std::vector<std::string>& tableColumnNames,
      const std::vector<TypePtr>& tableColumnTypes,
      const std::vector<std::string>& partitionedBy,
      std::shared_ptr<connector::hiveV2::HiveBucketProperty> bucketProperty,
      std::shared_ptr<connector::hiveV2::LocationHandle> locationHandle,
      const dwio::common::FileFormat tableStorageFormat =
          dwio::common::FileFormat::DWRF,
      const std::optional<common::CompressionKind> compressionKind = {},
      const std::unordered_map<std::string, std::string>& serdeParameters = {},
      const std::shared_ptr<dwio::common::WriterOptions>& writerOptions =
          nullptr,
      const bool ensureFiles = false);

  static std::shared_ptr<connector::hiveV2::HiveInsertTableHandle>
  makeHiveInsertTableHandle(
      const std::vector<std::string>& tableColumnNames,
      const std::vector<TypePtr>& tableColumnTypes,
      const std::vector<std::string>& partitionedBy,
      std::shared_ptr<connector::hiveV2::LocationHandle> locationHandle,
      const dwio::common::FileFormat tableStorageFormat =
          dwio::common::FileFormat::DWRF,
      const std::optional<common::CompressionKind> compressionKind = {},
      const std::shared_ptr<dwio::common::WriterOptions>& writerOptions =
          nullptr,
      const bool ensureFiles = false);

  static std::shared_ptr<connector::hiveV2::HiveColumnHandle> regularHiveColumn(
      const std::string& name,
      const TypePtr& type);

  static std::shared_ptr<connector::hiveV2::HiveColumnHandle> hivePartitionKey(
      const std::string& name,
      const TypePtr& type);

  static std::shared_ptr<connector::hiveV2::HiveColumnHandle>
  synthesizedHiveColumn(const std::string& name, const TypePtr& type);

  static ColumnHandleMap allRegularColumns(const RowTypePtr& rowType) {
    ColumnHandleMap assignments;
    assignments.reserve(rowType->size());
    for (uint32_t i = 0; i < rowType->size(); ++i) {
      const auto& name = rowType->nameOf(i);
      assignments[name] = regularHiveColumn(name, rowType->childAt(i));
    }
    return assignments;
  }
};

/// Same as connector::hiveV2::HiveConnectorBuilder, except that this defaults
/// connectorId to kHiveConnectorId.
class HiveConnectorSplitBuilder
    : public connector::hiveV2::HiveConnectorSplitBuilder {
 public:
  explicit HiveConnectorSplitBuilder(std::string filePath)
      : connector::hiveV2::HiveConnectorSplitBuilder(filePath) {
    connectorId(kHiveConnectorId);
  }
};

} // namespace facebook::velox::connector::hiveV2::test
