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
#include "velox/common/io/IoStatistics.h"
#include "velox/connectors/lakehouse/base/DataSourceBase.h"
#include "velox/connectors/lakehouse/base/FileHandle.h"
#include "velox/connectors/lakehouse/hiveV2/HiveConnectorSplit.h"
#include "velox/connectors/lakehouse/hiveV2/HivePartitionFunction.h"
#include "velox/connectors/lakehouse/hiveV2/HiveSplitReader.h"
#include "velox/connectors/lakehouse/hiveV2/HiveTableHandle.h"
#include "velox/dwio/common/Statistics.h"

namespace facebook::velox::connector::lakehouse::hive {

using FileHandleFactory = CachedFactory<
    std::string,
    base::FileHandle,
    base::FileHandleGenerator,
    FileProperties,
    filesystems::File::IoStats,
    base::FileHandleSizer>;

struct SpecialColumnNames {
  std::optional<std::string> rowIndex;
  std::optional<std::string> rowId;
};

class HiveDataSource : public base::DataSourceBase {
 public:
  HiveDataSource(
      const RowTypePtr& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      base::FileHandleFactory* fileHandleFactory,
      folly::Executor* executor,
      const ConnectorQueryCtx* connectorQueryCtx,
      const std::shared_ptr<HiveConfig>& hiveConfig);

  void addSplit(std::shared_ptr<ConnectorSplit> split) override;

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override;

  bool allPrefetchIssued() const override {
    return splitReader_ && splitReader_->allPrefetchIssued();
  }

  void setFromDataSource(std::unique_ptr<DataSource> sourceUnique) override;

  std::shared_ptr<wave::WaveDataSource> toWaveDataSource() override;

  using WaveDelegateHookFunction = std::function<std::shared_ptr<
      wave::WaveDataSource>(
      const std::shared_ptr<base::TableHandleBase>& hiveTableHandle,
      const std::shared_ptr<velox::common::ScanSpec>& scanSpec,
      const RowTypePtr& readerOutputType,
      std::unordered_map<std::string, std::shared_ptr<base::ColumnHandleBase>>*
          partitionKeys,
      FileHandleFactory* fileHandleFactory,
      folly::Executor* executor,
      const ConnectorQueryCtx* connectorQueryCtx,
      const std::shared_ptr<base::ConnectorConfigBase>& hiveConfig,
      const std::shared_ptr<io::IoStatistics>& ioStats,
      const exec::ExprSet* remainingFilter,
      std::shared_ptr<velox::common::MetadataFilter> metadataFilter)>;

  static WaveDelegateHookFunction waveDelegateHook_;

  static void registerWaveDelegateHook(WaveDelegateHookFunction hook);

  const ConnectorQueryCtx* testingConnectorQueryCtx() const {
    return connectorQueryCtx_;
  }

 private:
  std::shared_ptr<velox::common::ScanSpec> makeScanSpec() override;

  bool isSpecialColumn(const std::string& name) const override;
  void setupRowIdColumn();

  vector_size_t evaluateRemainingPartitionFilter(
      RowVectorPtr& rowVector,
      BufferPtr& remainingIndices) override;

  std::unique_ptr<HivePartitionFunction> setupBucketConversion();

  vector_size_t applyBucketConversion(
      const RowVectorPtr& rowVector,
      BufferPtr& indices);

  SpecialColumnNames specialColumns_{};
  int64_t numBucketConversion_ = 0;
  std::unique_ptr<HivePartitionFunction> partitionFunction_;
  std::vector<uint32_t> partitions_;

  // Remembers the WaveDataSource. Successive calls to toWaveDataSource() will
  // return the same.
  std::shared_ptr<wave::WaveDataSource> waveDataSource_;
};
} // namespace facebook::velox::connector::lakehouse::hive
