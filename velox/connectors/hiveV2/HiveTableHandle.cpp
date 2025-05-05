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

#include "velox/connectors/hiveV2/HiveTableHandle.h"

namespace facebook::velox::connector::hiveV2 {

folly::dynamic HiveColumnHandle::serialize() const {
  folly::dynamic obj = ColumnHandleBase::serializeBase("HiveColumnHandle");
  obj["hiveColumnHandleName"] = columnName_;
  obj["hiveType"] = hiveType_->serialize();
  return obj;
}

std::string HiveColumnHandle::toString() const {
  return ColumnHandleBase::toStringBase("HiveColumnHandle");
}

ColumnHandlePtr HiveColumnHandle::create(const folly::dynamic& obj) {
  auto columnName = obj["hiveColumnHandleName"].asString();
  auto columnType = columnTypeFromName(obj["columnType"].asString());
  auto dataType = ISerializable::deserialize<Type>(obj["dataType"]);
  auto hiveType = ISerializable::deserialize<Type>(obj["hiveType"]);

  const auto& arr = obj["requiredSubfields"];
  std::vector<velox::common::Subfield> requiredSubfields;
  requiredSubfields.reserve(arr.size());
  for (auto& s : arr) {
    requiredSubfields.emplace_back(s.asString());
  }

  return std::make_shared<HiveColumnHandle>(
      columnName,
      columnType,
      std::move(dataType),
      std::move(hiveType),
      std::move(requiredSubfields));
  ;
}

void HiveColumnHandle::registerSerDe() {
  auto& registry = DeserializationRegistryForSharedPtr();
  registry.Register("HiveColumnHandle", HiveColumnHandle::create);
}

std::string HiveTableHandle::toString() const {
  return TableHandleBase::toStringBase("HiveTableHandle");
}

folly::dynamic HiveTableHandle::serialize() const {
  return TableHandleBase::serializeBase("HiveTableHandle");
}

ConnectorTableHandlePtr HiveTableHandle::create(
    const folly::dynamic& obj,
    void* context) {
  return hive_common::TableHandleBase::create(obj, context);
}

void HiveTableHandle::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register("HiveTableHandle", create);
}

} // namespace facebook::velox::connector::hiveV2
