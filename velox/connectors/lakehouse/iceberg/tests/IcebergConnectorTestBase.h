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

#include "ConnectorTestBase.h"
#include "velox/connectors/lakehouse/iceberg/IcebergConnectorSplit.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/TempFilePath.h"

namespace facebook::velox::connector::lakehouse::iceberg::test {

class IcebergConnectorTestBase : public ConnectorTestBase {
 public:
  std::shared_ptr<exec::Task> assertQuery(
      const core::PlanNodePtr& plan,
      const std::vector<std::shared_ptr<exec::test::TempFilePath>>& filePaths,
      const std::string& duckDbSql);

  std::shared_ptr<exec::Task> assertQuery(
      const core::PlanNodePtr& plan,
      const std::vector<std::shared_ptr<ConnectorSplit>>& splits,
      const std::string& duckDbSql,
      const int32_t numPrefetchSplit = 0);

  std::vector<std::shared_ptr<ConnectorSplit>>
  makeIcebergConnectorSplits(
      const std::vector<std::shared_ptr<exec::test::TempFilePath>>& filePaths);

  /// Split file at path 'filePath' into 'splitCount' splits. If not local file,
  /// file size can be given as 'externalSize'.
  static std::vector<
      std::shared_ptr<ConnectorSplit>>
  makeIcebergConnectorSplits(
      const std::string& filePath,
      uint32_t splitCount,
      dwio::common::FileFormat format,
      const std::optional<
          std::unordered_map<std::string, std::optional<std::string>>>&
          partitionKeys = {},
      const std::optional<std::unordered_map<std::string, std::string>>&
          infoColumns = {});
};

} // namespace facebook::velox::connector::lakehouse::iceberg::test
