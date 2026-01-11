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

#include "FileHandle.h"
#include "IcebergConfig.h"
#include "velox/connectors/Connector.h"
#include "velox/core/PlanNode.h"
#include "velox/type/Type.h"

#include <folly/Executor.h>

namespace facebook::velox::connector::lakehouse::iceberg {

using namespace facebook::velox::connector;

class IcebergConnector : public Connector {
 public:
  IcebergConnector(
      const std::string& id,
      std::shared_ptr<const facebook::velox::config::ConfigBase> config,
      folly::Executor* executor);

  const std::shared_ptr<const facebook::velox::config::ConfigBase>&
  connectorConfig() const {
    return icebergConfig_->config();
  }

  bool canAddDynamicFilter() const override {
    return true;
  }

  std::unique_ptr<DataSource> createDataSource(
      const RowTypePtr& outputType,
      const ConnectorTableHandlePtr& tableHandle,
      const connector::ColumnHandleMap& columnHandles,
      ConnectorQueryCtx* connectorQueryCtx) override;

  bool supportsSplitPreload() const override {
    return true;
  }

  std::unique_ptr<DataSink> createDataSink(
      RowTypePtr inputType,
      ConnectorInsertTableHandlePtr connectorInsertTableHandle,
      ConnectorQueryCtx* connectorQueryCtx,
      CommitStrategy commitStrategy) override final;

  folly::Executor* executor() const override {
    return executor_;
  }

  FileHandleCacheStats fileHandleCacheStats() {
    return fileHandleFactory_.cacheStats();
  }

  // NOTE: this is to clear file handle cache which might affect performance,
  // and is only used for operational purposes.
  FileHandleCacheStats clearFileHandleCache() {
    return fileHandleFactory_.clearCache();
  }

 protected:
  const std::shared_ptr<IcebergConfig> icebergConfig_;
  FileHandleFactory fileHandleFactory_;
  folly::Executor* executor_;
};

class IcebergConnectorFactory : public ConnectorFactory {
 public:
  static constexpr const char* kIcebergConnectorName = "iceberg";

  IcebergConnectorFactory() : ConnectorFactory(kIcebergConnectorName) {}

  explicit IcebergConnectorFactory(const char* connectorName)
      : ConnectorFactory(connectorName) {}

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const velox::config::ConfigBase> config,
      folly::Executor* ioExecutor = nullptr,
      folly::Executor* cpuExecutor = nullptr) override {
    return std::make_shared<IcebergConnector>(id, config, ioExecutor);
  }
};

// TODO: Support multiple versioned IcebergPartitionFunctionSpec. Iceberg
// partition spec can be different for different partitions. E.g. some old
// partitions may be partitioned by DAY(ds), and new partitions are changed to
// MONTH(ds). Iceberg table metadata keeps all partition specs. But now only the
// default partition spec is passed to Velox in the plan fragment now.
class IcebergPartitionFunctionSpec : public velox::core::PartitionFunctionSpec {
 public:
  IcebergPartitionFunctionSpec(
      int numBuckets,
      std::vector<int> bucketToPartition,
      std::vector<column_index_t> channels,
      std::vector<VectorPtr> constValues)
      : numBuckets_(numBuckets),
        bucketToPartition_(std::move(bucketToPartition)),
        channels_(std::move(channels)),
        constValues_(std::move(constValues)) {}

  /// The constructor without 'bucketToPartition' input is used in case that
  /// we don't know the actual number of partitions until we create the
  /// partition function instance. The hive partition function spec then builds
  /// a bucket to partition map based on the actual number of partitions with
  /// round-robin partitioning scheme to create the function instance. For
  /// instance, when we create the local partition node with hive bucket
  /// function to support multiple table writer drivers, we don't know the the
  /// actual number of table writer drivers until start the task.
  IcebergPartitionFunctionSpec(
      int numBuckets,
      std::vector<column_index_t> channels,
      std::vector<VectorPtr> constValues)
      : IcebergPartitionFunctionSpec(
            numBuckets,
            {},
            std::move(channels),
            std::move(constValues)) {}

  std::unique_ptr<core::PartitionFunction> create(
      int numPartitions,
      bool localExchange) const override;

  std::string toString() const override;

  folly::dynamic serialize() const override;

  static core::PartitionFunctionSpecPtr deserialize(
      const folly::dynamic& obj,
      void* context);

 private:
  const int numBuckets_;
  const std::vector<int> bucketToPartition_;
  const std::vector<column_index_t> channels_;
  const std::vector<VectorPtr> constValues_;
};

void registerIcebergPartitionFunctionSerDe();

/// Hook for connecting metadata functions to a IcebergConnector. Each
/// registered factory is called after initializing a IcebergConnector until one
/// of these returns a ConnectorMetadata instance.
class IcebergConnectorMetadataFactory {
 public:
  virtual ~IcebergConnectorMetadataFactory() = default;
};

bool registerIcebergConnectorMetadataFactory(
    std::unique_ptr<IcebergConnectorMetadataFactory>);

} // namespace facebook::velox::connector::lakehouse::iceberg
