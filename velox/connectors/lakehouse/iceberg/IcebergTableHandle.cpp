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

#include "IcebergTableHandle.h"

namespace facebook::velox::connector::lakehouse::iceberg  {

folly::dynamic IcebergColumnHandle::serialize() const {
 folly::dynamic obj = ColumnHandleBase::serializeBase("IcebergColumnHandle");
 obj["icebergColumnHandleName"] = columnName_;
 return obj;
}

std::string IcebergColumnHandle::toString() const {
 return ColumnHandleBase::toStringBase("IcebergColumnHandle");
}

ColumnHandlePtr IcebergColumnHandle::create(const folly::dynamic& obj) {
 auto columnName = obj["icebergColumnHandleName"].asString();
 auto columnType = columnTypeFromName(obj["columnType"].asString());
 auto dataType = ISerializable::deserialize<Type>(obj["dataType"]);

 const auto& arr = obj["requiredSubfields"];
 std::vector<velox::common::Subfield> requiredSubfields;
 requiredSubfields.reserve(arr.size());
 for (auto& s : arr) {
   requiredSubfields.emplace_back(s.asString());
 }

 return std::make_shared<IcebergColumnHandle>(
     columnName,
     columnType,
     std::move(dataType),
     std::move(requiredSubfields));
 ;
}

void IcebergColumnHandle::registerSerDe() {
 auto& registry = DeserializationRegistryForSharedPtr();
 registry.Register("IcebergColumnHandle", IcebergColumnHandle::create);
}

std::string IcebergTableHandle::toString() const {
 return TableHandleBase::toStringBase("IcebergTableHandle");
}

folly::dynamic IcebergTableHandle::serialize() const {
 return TableHandleBase::serializeBase("IcebergTableHandle");
}

ConnectorTableHandlePtr IcebergTableHandle::create(
   const folly::dynamic& obj,
   void* context) {
 return base::TableHandleBase::create(obj, context);
}

void IcebergTableHandle::registerSerDe() {
 auto& registry = DeserializationWithContextRegistryForSharedPtr();
 registry.Register("IcebergTableHandle", create);
}

} // namespace facebook::velox::connector::lakehouse::iceberg
