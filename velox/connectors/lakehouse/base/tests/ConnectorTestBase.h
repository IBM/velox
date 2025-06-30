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

#include "velox/connectors/lakehouse/base/TableHandleBase.h"
#include "velox/dwio/dwrf/common/Config.h"
#include "velox/dwio/dwrf/writer/FlushPolicy.h"
#include "velox/exec/Operator.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/TempFilePath.h"
#include "velox/type/tests/SubfieldFiltersBuilder.h"

#include <string>

namespace facebook::velox::connector::lakehouse::base::test {

using ColumnHandleMap =
    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>;

class ConnectorTestBase : public exec::test::OperatorTestBase {
 public:
  ConnectorTestBase();

  void SetUp() override;
  void TearDown() override;

  void resetHiveConnector(
      const std::shared_ptr<const config::ConfigBase>& config);

  void writeToFiles(
      const std::vector<std::string>& filePaths,
      std::vector<RowVectorPtr> vectors);

  void writeToFile(const std::string& filePath, RowVectorPtr vector);

  void writeToFile(
      const std::string& filePath,
      const std::vector<RowVectorPtr>& vectors,
      std::shared_ptr<dwrf::Config> config =
          std::make_shared<facebook::velox::dwrf::Config>(),
      const std::function<std::unique_ptr<dwrf::DWRFFlushPolicy>()>&
          flushPolicyFactory = nullptr);

  void writeToFile(
      const std::string& filePath,
      const std::vector<RowVectorPtr>& vectors,
      std::shared_ptr<dwrf::Config> config,
      const TypePtr& schema,
      const std::function<std::unique_ptr<dwrf::DWRFFlushPolicy>()>&
          flushPolicyFactory = nullptr);

  // Creates a directory using matching file system based on directoryPath.
  // No throw when directory already exists.
  void createDirectory(const std::string& directoryPath);

  // Removes a directory using matching file system based on directoryPath.
  // No op when directory does not exist.
  void removeDirectory(const std::string& directoryPath);

  // Removes a file using matching file system based on filePath.
  // No op when file does not exist.
  void removeFile(const std::string& filePath);

  std::vector<RowVectorPtr> makeVectors(
      const RowTypePtr& rowType,
      int32_t numVectors,
      int32_t rowsPerVector);

  using OperatorTestBase::assertQuery;

//  std::shared_ptr<velox::exec::Task> assertQuery(
//      const core::PlanNodePtr& plan,
//      const std::vector<std::shared_ptr<connector::ConnectorSplit>>& splits,
//      const std::string& duckDbSql,
//      const int32_t numPrefetchSplit);

  static std::vector<std::shared_ptr<facebook::velox::exec::test::TempFilePath>>
  makeFilePaths(int count);

  static std::shared_ptr<base::ColumnHandleBase> regularColumn(
      const std::string& name,
      const TypePtr& type);

  static std::shared_ptr<base::ColumnHandleBase> partitionKey(
      const std::string& name,
      const TypePtr& type);

  static std::shared_ptr<base::ColumnHandleBase>
  synthesizedColumn(const std::string& name, const TypePtr& type);

  static ColumnHandleMap allRegularColumns(const RowTypePtr& rowType) {
    ColumnHandleMap assignments;
    assignments.reserve(rowType->size());
    for (uint32_t i = 0; i < rowType->size(); ++i) {
      const auto& name = rowType->nameOf(i);
      assignments[name] = regularColumn(name, rowType->childAt(i));
    }
    return assignments;
  }
};

///// Same as connector::hive::HiveConnectorBuilder, except that this defaults
///// connectorId to kHiveConnectorId.
// class HiveConnectorSplitBuilder
//     : public connector::base::ConnectorSplitBaseBuilder {
//  public:
//   explicit HiveConnectorSplitBuilder(std::string filePath)
//       : connector::base::ConnectorSplitBaseBuilder(filePath) {
//     connectorId(kHiveConnectorId);
//   }
// };

} // namespace facebook::velox::connector::lakehouse::base::test
