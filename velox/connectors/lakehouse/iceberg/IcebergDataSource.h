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
#include "DataSourceBase.h"
#include "FileHandle.h"
#include "velox/common/base/RandomUtil.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/connectors/lakehouse/iceberg/IcebergConnectorSplit.h"
#include "velox/connectors/lakehouse/iceberg/IcebergPartitionFunction.h"
#include "velox/connectors/lakehouse/iceberg/IcebergSplitReader.h"
#include "velox/connectors/lakehouse/iceberg/IcebergTableHandle.h"
#include "velox/dwio/common/Statistics.h"

namespace facebook::velox::connector::lakehouse::iceberg {

namespace {

bool isMember(
    const std::vector<exec::FieldReference*>& fields,
    const exec::FieldReference& field);

bool shouldEagerlyMaterialize(
    const exec::Expr& remainingFilter,
    const exec::FieldReference& field);

}

class IcebergDataSource : public DataSourceBase {
 public:
  IcebergDataSource(
      const RowTypePtr& outputType,
      const ConnectorTableHandlePtr& tableHandle,
      const connector::ColumnHandleMap& columnHandles,
      FileHandleFactory* fileHandleFactory,
      folly::Executor* executor,
      const ConnectorQueryCtx* connectorQueryCtx,
      const std::shared_ptr<ConnectorConfigBase>& connectorConfig);

  void addSplit(std::shared_ptr<ConnectorSplit> split) override;

  std::optional<RowVectorPtr> next(uint64_t size, velox::ContinueFuture& future)
      override;

  const ConnectorQueryCtx* testingConnectorQueryCtx() const {
    return connectorQueryCtx_;
  }

 private:
  std::shared_ptr<velox::common::ScanSpec> makeScanSpec();

  bool isSpecialColumn(const std::string& name) const override;
  void setupRowIdColumn();

  vector_size_t evaluateRemainingPartitionFilter(
      RowVectorPtr& rowVector,
      BufferPtr& remainingIndices) override;

  std::unique_ptr<IcebergPartitionFunction> partitionFunction_;
  std::vector<uint32_t> partitions_;
};
} // namespace facebook::velox::connector::lakehouse::iceberg
