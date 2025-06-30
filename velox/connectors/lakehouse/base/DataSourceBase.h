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

#include "ConnectorConfigBase.h"
#include "ConnectorSplitBase.h"
#include "ConnectorUtil.h"
#include "FileHandle.h"
#include "SplitReaderBase.h"
#include "velox/connectors/Connector.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/type/Type.h"

namespace facebook::velox::connector::lakehouse::base {

class DataSourceBase : public DataSource {
 public:
  DataSourceBase(
      const RowTypePtr& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      FileHandleFactory* fileHandleFactory,
      folly::Executor* executor,
      const ConnectorQueryCtx* connectorQueryCtx,
      const std::shared_ptr<ConnectorConfigBase>& ConnectorConfigBase);

  void addSplit(std::shared_ptr<ConnectorSplit> split) override = 0;

  std::optional<RowVectorPtr> next(uint64_t size, velox::ContinueFuture& future)
      override;

  void addDynamicFilter(
      column_index_t outputChannel,
      const std::shared_ptr<velox::common::Filter>& filter) override;

  uint64_t getCompletedBytes() override {
    return ioStats_->rawBytesRead();
  }

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override;

  bool allPrefetchIssued() const override {
    return splitReader_ && splitReader_->allPrefetchIssued();
  }

  void setFromDataSource(std::unique_ptr<DataSource> sourceUnique) override;

  int64_t estimatedRowSize() override;

 protected:
  virtual std::shared_ptr<velox::common::ScanSpec> makeScanSpec();

  virtual bool isSpecialColumn(const std::string& name) const {
    VELOX_UNREACHABLE();
  }

  virtual bool hasRemainingPartitionFilter() {
    return false;
  }

  virtual vector_size_t evaluateRemainingPartitionFilter(
      RowVectorPtr& rowVector,
      BufferPtr& remainingIndices) {
    return rowVector->size();
  }

  // Evaluates remainingFilter_ on the specified vector. Returns number of rows
  // passed. Populates filterEvalCtx_.selectedIndices and selectedBits if only
  // some rows passed the filter. If none or all rows passed
  // filterEvalCtx_.selectedIndices and selectedBits are not updated.
  vector_size_t evaluateRemainingFilter(RowVectorPtr& rowVector);

  const RowVectorPtr& getEmptyOutput() {
    if (!emptyOutput_) {
      emptyOutput_ = RowVector::createEmpty(outputType_, pool_);
    }
    return emptyOutput_;
  }

  void resetSplit();

  const ConnectorQueryCtx* const connectorQueryCtx_;
  FileHandleFactory* const fileHandleFactory_;
  folly::Executor* const executor_;
  core::ExpressionEvaluator* const expressionEvaluator_;
  const std::shared_ptr<ConnectorConfigBase> ConnectorConfigBase_;
  memory::MemoryPool* const pool_;

  // The row type for the data source output, not including filter-only columns
  const RowTypePtr outputType_;
  // Output type from file reader.  This is different from outputType_ that it
  // contains column names before assignment, and columns that only used in
  // remaining filter.
  RowTypePtr readerOutputType_;
  folly::F14FastMap<std::string, std::vector<const velox::common::Subfield*>>
      subfields_;

  std::shared_ptr<TableHandleBase> tableHandle_;
  // Column handles for the partition key columns keyed on partition key column
  // name. It comes from the TableScanNode's assignments.
  std::unordered_map<std::string, std::shared_ptr<ColumnHandleBase>>
      partitionColumnHandles_;
  // Column handles for the Split info columns keyed on their column names.
  std::unordered_map<std::string, std::shared_ptr<ColumnHandleBase>>
      infoColumns_;
  //  std::unordered_map<std::string, std::shared_ptr<ColumnHandleBase>>
  //      specialColumns_;
  //  SpecialColumnNames specialColumns_{};
  std::shared_ptr<velox::common::ScanSpec> scanSpec_;
  std::shared_ptr<ConnectorSplitBase> split_;
  std::unique_ptr<SplitReaderBase> splitReader_;

  VectorPtr output_;

  std::shared_ptr<io::IoStatistics> ioStats_;
  std::shared_ptr<filesystems::File::IoStats> fsStats_;
  dwio::common::RuntimeStatistics runtimeStats_;
  std::atomic<uint64_t> totalRemainingFilterTime_{0};
  uint64_t completedRows_ = 0;

  velox::common::SubfieldFilters filters_;
  std::shared_ptr<velox::common::MetadataFilter> metadataFilter_;
  std::shared_ptr<exec::ExprSet> remainingFilterExprSet_;
  RowVectorPtr emptyOutput_;

  // Field indices referenced in both remaining filter and output type. These
  // columns need to be materialized eagerly to avoid missing values in output.
  std::vector<column_index_t> multiReferencedFields_;

  std::shared_ptr<random::RandomSkipTracker> randomSkip_;

  //  int64_t numBucketConversion_ = 0;
  //  std::unique_ptr<HivePartitionFunction> partitionFunction_;
  std::vector<uint32_t> partitions_;

  // Reusable memory for remaining filter evaluation.
  VectorPtr filterResult_;
  SelectivityVector filterRows_;
  DecodedVector filterLazyDecoded_;
  SelectivityVector filterLazyBaseRows_;
  exec::FilterEvalCtx filterEvalCtx_;
};

} // namespace facebook::velox::connector::lakehouse::base
