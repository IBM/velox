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

#include "velox/connectors/hiveV2/HiveSplitReader.h"

#include "velox/connectors/hiveV2/HiveConfig.h"
#include "velox/connectors/hiveV2/HiveConnectorSplit.h"
#include "velox/connectors/hiveV2/HiveConnectorUtil.h"
#include "velox/connectors/hive_common/ConnectorUtil.h"
#include "velox/dwio/common/CachedBufferedInput.h"
#include "velox/dwio/common/ReaderFactory.h"

using namespace facebook::velox::common;

namespace facebook::velox::connector::hiveV2 {

HiveSplitReader::HiveSplitReader(
    const std::shared_ptr<const hive_common::ConnectorSplitBase>& split,
    const std::shared_ptr<const hive_common::TableHandleBase>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<hive_common::ColumnHandleBase>>* partitionKeys,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<const hive_common::ConnectorConfig>& connectorConfig,
    const RowTypePtr& readerOutputType,
    const std::shared_ptr<io::IoStatistics>& ioStats,
    const std::shared_ptr<filesystems::File::IoStats>& fsStats,
    hive_common::FileHandleFactory* fileHandleFactory,
    folly::Executor* executor,
    const std::shared_ptr<ScanSpec>& scanSpec)
    : SplitReader(
          split,
          tableHandle,
          partitionKeys,
          connectorQueryCtx,
          connectorConfig,
          readerOutputType,
          ioStats,
          fsStats,
          fileHandleFactory,
          executor,
          scanSpec) {}

HiveSplitReader::~HiveSplitReader() {}

std::string HiveSplitReader::toString() const {
  return SplitReader::toStringBase("HiveSplitReader");
}

bool HiveSplitReader::filterSplit(
    dwio::common::RuntimeStatistics& runtimeStats) const {
  return hiveV2::filterSplit(
        scanSpec_.get(),
        baseReader_.get(),
        split_->filePath,
        split_->partitionKeys,
        *partitionColumnHandles_,
        connectorConfig_->readTimestampPartitionValueAsLocalTime(
            connectorQueryCtx_->sessionProperties()));
}

std::vector<TypePtr> HiveSplitReader::adaptColumns(
    const RowTypePtr& fileType,
    const std::shared_ptr<const velox::RowType>& tableSchema) const {
  auto& childrenSpecs = scanSpec_->children();
  for (size_t i = 0; i < childrenSpecs.size(); ++i) {
    auto* childSpec = childrenSpecs[i].get();
    const std::string& fieldName = childSpec->fieldName();

    if (auto it = split_->partitionKeys.find(fieldName);
        it != split_->partitionKeys.end()) {
      setPartitionValue(childSpec, fieldName, it->second);
    }
  }

  return SplitReader::adaptColumns(fileType, tableSchema);
}

void HiveSplitReader::setPartitionValue(
    velox::common::ScanSpec* spec,
    const std::string& partitionKey,
    const std::optional<std::string>& value) const {
  auto it = partitionColumnHandles_->find(partitionKey);
  VELOX_CHECK(
      it != partitionColumnHandles_->end(),
      "ColumnHandle is missing for partition key {}",
      partitionKey);
  auto type = it->second->dataType();
  auto constant = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
      hive_common::newConstantFromString,
      type->kind(),
      type,
      value,
      1,
      connectorQueryCtx_->memoryPool(),
      connectorQueryCtx_->sessionTimezone(),
      connectorConfig_->readTimestampPartitionValueAsLocalTime(
          connectorQueryCtx_->sessionProperties()),
      false);
  spec->setConstantValue(constant);
}

} // namespace facebook::velox::connector::hiveV2
