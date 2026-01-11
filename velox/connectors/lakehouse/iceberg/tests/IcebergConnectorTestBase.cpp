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

#include "velox/connectors/lakehouse/iceberg/tests/IcebergConnectorTestBase.h"

#include "velox/exec/tests/utils/AssertQueryBuilder.h"

namespace facebook::velox::connector::lakehouse::iceberg::test {

std::shared_ptr<exec::Task> IcebergConnectorTestBase::assertQuery(
    const core::PlanNodePtr& plan,
    const std::vector<std::shared_ptr<exec::test::TempFilePath>>& filePaths,
    const std::string& duckDbSql) {
  return OperatorTestBase::assertQuery(
      plan, makeIcebergConnectorSplits(filePaths), duckDbSql);
}

std::shared_ptr<exec::Task> IcebergConnectorTestBase::assertQuery(
    const core::PlanNodePtr& plan,
    const std::vector<std::shared_ptr<ConnectorSplit>>& splits,
    const std::string& duckDbSql,
    const int32_t numPrefetchSplit) {
  return exec::test::AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(
          core::QueryConfig::kMaxSplitPreloadPerDriver,
          std::to_string(numPrefetchSplit))
      .splits(splits)
      .assertResults(duckDbSql);
}

std::vector<std::shared_ptr<ConnectorSplit>>
IcebergConnectorTestBase::makeIcebergConnectorSplits(
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
  const uint64_t fileSize = file->size();
  // Take the upper bound.
  const uint64_t splitSize = std::ceil((fileSize) / splitCount);
  std::vector<std::shared_ptr<ConnectorSplit>> splits;
  // Add all the splits.
  for (uint32_t i = 0; i < splitCount; i++) {
    auto splitBuilder = IcebergConnectorSplitBuilder(filePath)
                            .fileFormat(format)
                            .start(i * splitSize)
                            .length(splitSize);
    if (infoColumns.has_value()) {
      for (const auto& infoColumn : infoColumns.value()) {
        splitBuilder.infoColumn(infoColumn.first, infoColumn.second);
      }
    }
    if (partitionKeys.has_value()) {
      for (const auto& partitionKey : partitionKeys.value()) {
        splitBuilder.partitionKey(partitionKey.first, partitionKey.second);
      }
    }

    auto split = splitBuilder.build();
    splits.push_back(std::move(split));
  }
  return splits;
}

std::vector<std::shared_ptr<ConnectorSplit>>
IcebergConnectorTestBase::makeIcebergConnectorSplits(
    const std::vector<std::shared_ptr<exec::test::TempFilePath>>& filePaths) {
  std::vector<std::shared_ptr<ConnectorSplit>> splits;
  splits.reserve(filePaths.size());
  for (const auto& filePath : filePaths) {
    IcebergConnectorSplitBuilder icebergConnectorSplitBuilder(filePath->getPath());
    icebergConnectorSplitBuilder.start(0)
        .length(std::numeric_limits<uint64_t>::max());
    splits.push_back(icebergConnectorSplitBuilder.build());
  }
  return splits;
}

}
