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

#include "velox/connectors/iceberg/IcebergConnector.h"

#include "velox/common/base/Fs.h"
#include "velox/connectors/iceberg/IcebergConfig.h"
#include "velox/connectors/iceberg/IcebergDataSink.h"
#include "velox/connectors/iceberg/IcebergDataSource.h"
#include "velox/connectors/iceberg/IcebergPartitionFunction.h"
#include "velox/expression/ExprToSubfieldFilter.h"
#include "velox/expression/FieldReference.h"

#include <boost/lexical_cast.hpp>
#include <folly/Executor.h>

#include <memory>

namespace facebook::velox::connector::iceberg {

using namespace facebook::velox::connector;
using namespace facebook::velox::connector::hive_common;
using namespace facebook::velox::exec;

namespace {
std::vector<std::unique_ptr<IcebergConnectorMetadataFactory>>&
IcebergConnectorMetadataFactories() {
  static std::vector<std::unique_ptr<IcebergConnectorMetadataFactory>>
      factories;
  return factories;
}
} // namespace

IcebergConnector::IcebergConnector(
    const std::string& id,
    std::shared_ptr<const config::ConfigBase> config,
    folly::Executor* executor)
    : Connector(id),
      icebergConfig_(std::make_shared<IcebergConfig>(config)),
      fileHandleFactory_(
          icebergConfig_->isFileHandleCacheEnabled()
              ? std::make_unique<
                    SimpleLRUCache<std::string, hive_common::FileHandle>>(
                    icebergConfig_->numCacheFileHandles())
              : nullptr,
          std::make_unique<hive_common::FileHandleGenerator>(config)),
      executor_(executor) {
  if (icebergConfig_->isFileHandleCacheEnabled()) {
    LOG(INFO) << "Iceberg connector " << connectorId()
              << " created with maximum of "
              << icebergConfig_->numCacheFileHandles()
              << " cached file handles with expiration of "
              << icebergConfig_->fileHandleExpirationDurationMs() << "ms.";
  } else {
    LOG(INFO) << "Iceberg connector " << connectorId()
              << " created with file handle cache disabled";
  }
  for (auto& factory : IcebergConnectorMetadataFactories()) {
    metadata_ = factory->create(this);
    if (metadata_ != nullptr) {
      break;
    }
  }
}

std::unique_ptr<DataSource> IcebergConnector::createDataSource(
    const RowTypePtr& outputType,
    const std::shared_ptr<ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    ConnectorQueryCtx* connectorQueryCtx) {
  return std::make_unique<IcebergDataSource>(
      outputType,
      tableHandle,
      columnHandles,
      &fileHandleFactory_,
      executor_,
      connectorQueryCtx,
      icebergConfig_);
}

std::unique_ptr<DataSink> IcebergConnector::createDataSink(
    RowTypePtr inputType,
    std::shared_ptr<ConnectorInsertTableHandle> connectorInsertTableHandle,
    ConnectorQueryCtx* connectorQueryCtx,
    CommitStrategy commitStrategy) {
  return std::make_unique<IcebergDataSink>(
      inputType,
      connectorInsertTableHandle,
      connectorQueryCtx,
      commitStrategy,
      icebergConfig_);
}

// static
// TODO: change to Iceberg semantics
std::unique_ptr<core::PartitionFunction> IcebergPartitionFunctionSpec::create(
    int numPartitions,
    bool localExchange) const {
  std::vector<int> bucketToPartitions;
  if (bucketToPartition_.empty()) {
    // NOTE: if hive partition function spec doesn't specify bucket to partition
    // mapping, then we do round-robin mapping based on the actual number of
    // partitions.
    bucketToPartitions.resize(numBuckets_);
    for (int bucket = 0; bucket < numBuckets_; ++bucket) {
      bucketToPartitions[bucket] = bucket % numPartitions;
    }
    if (localExchange) {
      // Shuffle the map from bucket to partition for local exchange so we don't
      // use the same map for remote shuffle.
      std::shuffle(
          bucketToPartitions.begin(),
          bucketToPartitions.end(),
          std::mt19937{0});
    }
  }
  return std::make_unique<velox::connector::iceberg::IcebergPartitionFunction>(
      numBuckets_,
      bucketToPartition_.empty() ? std::move(bucketToPartitions)
                                 : bucketToPartition_,
      channels_,
      constValues_);
}

// TODO: change to Iceberg semantics
std::string IcebergPartitionFunctionSpec::toString() const {
  std::ostringstream keys;
  size_t constIndex = 0;
  for (auto i = 0; i < channels_.size(); ++i) {
    if (i > 0) {
      keys << ", ";
    }
    auto channel = channels_[i];
    if (channel == kConstantChannel) {
      keys << "\"" << constValues_[constIndex++]->toString(0) << "\"";
    } else {
      keys << channel;
    }
  }

  return fmt::format("Iceberg (({}) buckets: {})", keys.str(), numBuckets_);
}

// TODO: change to Iceberg semantics
folly::dynamic IcebergPartitionFunctionSpec::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "IcebergPartitionFunctionSpec";
  obj["numBuckets"] = ISerializable::serialize(numBuckets_);
  obj["bucketToPartition"] = ISerializable::serialize(bucketToPartition_);
  obj["keys"] = ISerializable::serialize(channels_);
  std::vector<velox::core::ConstantTypedExpr> constValueExprs;
  constValueExprs.reserve(constValues_.size());
  for (const auto& value : constValues_) {
    constValueExprs.emplace_back(value);
  }
  obj["constants"] = ISerializable::serialize(constValueExprs);
  return obj;
}

// static
// TODO: change to Iceberg semantics
core::PartitionFunctionSpecPtr IcebergPartitionFunctionSpec::deserialize(
    const folly::dynamic& obj,
    void* context) {
  std::vector<column_index_t> channels =
      ISerializable::deserialize<std::vector<column_index_t>>(
          obj["keys"], context);
  const auto constTypedValues =
      ISerializable::deserialize<std::vector<velox::core::ConstantTypedExpr>>(
          obj["constants"], context);
  std::vector<VectorPtr> constValues;
  constValues.reserve(constTypedValues.size());
  auto* pool = static_cast<memory::MemoryPool*>(context);
  for (const auto& value : constTypedValues) {
    constValues.emplace_back(value->toConstantVector(pool));
  }
  return std::make_shared<IcebergPartitionFunctionSpec>(
      ISerializable::deserialize<int>(obj["numBuckets"], context),
      ISerializable::deserialize<std::vector<int>>(
          obj["bucketToPartition"], context),
      std::move(channels),
      std::move(constValues));
}

void registerIcebergPartitionFunctionSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register(
      "IcebergPartitionFunctionSpec",
      IcebergPartitionFunctionSpec::deserialize);
}

bool registerIcebergConnectorMetadataFactory(
    std::unique_ptr<IcebergConnectorMetadataFactory> factory) {
  IcebergConnectorMetadataFactories().push_back(std::move(factory));
  return true;
}

} // namespace facebook::velox::connector::iceberg
