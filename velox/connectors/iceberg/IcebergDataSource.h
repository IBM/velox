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
// #include "velox/connectors/Connector.h"
#include "velox/connectors/iceberg/IcebergConnectorSplit.h"
 #include "velox/connectors/hive_common/ConnectorConfig.h"
#include "velox/connectors/hive_common/ConnectorDataSource.h"
#include "velox/connectors/hive_common/FileHandle.h"
// #include "velox/connectors/iceberg/IcebergConnectorUtil.h"
#include "velox/connectors/iceberg/IcebergPartitionFunction.h"
#include "velox/connectors/iceberg/IcebergSplitReader.h"
#include "velox/connectors/iceberg/IcebergTableHandle.h"
#include "velox/dwio/common/Statistics.h"
//  #include "velox/expression/Expr.h"

namespace facebook::velox::connector::iceberg {

// class IcebergConfig;

using FileHandleFactory = CachedFactory<
    std::string,
    hive_common::FileHandle,
    hive_common::FileHandleGenerator,
    FileProperties,
    filesystems::File::IoStats,
    hive_common::FileHandleSizer>;

class IcebergDataSource : public hive_common::ConnectorDataSource {
 public:
  IcebergDataSource(
      const RowTypePtr& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      FileHandleFactory* fileHandleFactory,
      folly::Executor* executor,
      const ConnectorQueryCtx* connectorQueryCtx,
      const std::shared_ptr<IcebergConfig>& icebergConfig);

  void addSplit(std::shared_ptr<ConnectorSplit> split) override;

  std::optional<RowVectorPtr> next(uint64_t size, velox::ContinueFuture& future)
      override;

  //  void addDynamicFilter(
  //      column_index_t outputChannel,
  //      const std::shared_ptr<Filter>& filter) override;

      //  std::unordered_map<std::string, RuntimeCounter> runtimeStats()
      //  override;

      //  bool allPrefetchIssued() const override {
      //    return splitReader_ && splitReader_->allPrefetchIssued();
      //  }

      //  void setFromDataSource(std::unique_ptr<DataSource> sourceUnique)
      //  override;

      //  int64_t estimatedRowSize() override;

      const ConnectorQueryCtx*
      testingConnectorQueryCtx() const {
    return connectorQueryCtx_;
  }

 private:
  std::shared_ptr<velox::common::ScanSpec> makeScanSpec() override;
  //
  //
  //  memory::MemoryPool* const pool_;
  //
  //  std::shared_ptr<IcebergConnectorSplit> split_;
  //  std::shared_ptr<IcebergTableHandle> icebergTableHandle_;
  //  std::shared_ptr<ScanSpec> scanSpec_;
  //  VectorPtr output_;
  //  std::unique_ptr<SplitReader> splitReader_;
  //
  //  // Output type from file reader.  This is different from outputType_ that
  //  it
  //  // contains column names before assignment, and columns that only used in
  //  // remaining filter.
  //  RowTypePtr readerOutputType_;
  //
  //  // Column handles for the partition key columns keyed on partition key
  //  column
  //  // name.
  //  std::unordered_map<std::string, std::shared_ptr<IcebergColumnHandle>>
  //      partitionKeys_;
  //
  //  std::shared_ptr<io::IoStatistics> ioStats_;
  //  std::shared_ptr<filesystems::File::IoStats> fsStats_;

  bool isSpecialColumn(const std::string& name) const override;
  void setupRowIdColumn();

  vector_size_t evaluatePartitionFilter(
      RowVectorPtr& rowVector,
      BufferPtr& remainingIndices) override;

  std::unique_ptr<IcebergPartitionFunction> partitionFunction_;
  std::vector<uint32_t> partitions_;

  //  // Reusable memory for remaining filter evaluation.
  //  VectorPtr filterResult_;
  //  SelectivityVector filterRows_;
  //  DecodedVector filterLazyDecoded_;
  //  SelectivityVector filterLazyBaseRows_;
  //  exec::FilterEvalCtx filterEvalCtx_;

  // Remembers the WaveDataSource. Successive calls to toWaveDataSource() will
  // return the same.
//  std::shared_ptr<wave::WaveDataSource> waveDataSource_;
};
} // namespace facebook::velox::connector::iceberg
