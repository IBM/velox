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

#include "velox/common/base/RandomUtil.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/lakehouse/base/SplitReaderBase.h"
#include "velox/connectors/lakehouse/base/TableHandleBase.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/Reader.h"

namespace facebook::velox::connector::lakehouse::hive {

struct HiveConnectorSplit;
class HiveTableHandle;
class HiveColumnHandle;
class HiveConfig;

class HiveSplitReader : public base::SplitReaderBase {
 public:
  HiveSplitReader(
      const std::shared_ptr<const base::ConnectorSplitBase>& split,
      const std::shared_ptr<const base::TableHandleBase>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<base::ColumnHandleBase>>* partitionKeys,
      const ConnectorQueryCtx* connectorQueryCtx,
      const std::shared_ptr<const base::ConnectorConfigBase>&
          ConnectorConfigBase,
      const RowTypePtr& readerOutputType,
      const std::shared_ptr<io::IoStatistics>& ioStats,
      const std::shared_ptr<filesystems::File::IoStats>& fsStats,
      base::FileHandleFactory* fileHandleFactory,
      folly::Executor* executor,
      const std::shared_ptr<velox::common::ScanSpec>& scanSpec);

   ~HiveSplitReader() override;

  std::string toString() const;

 private:
  bool filterSplit(
      dwio::common::RuntimeStatistics& runtimeStats) const override;

  /// Different table formats may have different meatadata columns.
  /// This function will be used to update the scanSpec for these columns.
  std::vector<TypePtr> adaptColumns(
      const RowTypePtr& fileType,
      const std::shared_ptr<const velox::RowType>& tableSchema) const override;

  /// In Hive, partitions are organized as directories, and partition columns
  /// are not written in the data files. So a partition column would be read as
  /// constant value.
  void setPartitionValue(
      velox::common::ScanSpec* spec,
      const std::string& partitionKey,
      const std::optional<std::string>& value) const;
};

} // namespace facebook::velox::connector::lakehouse::hive
