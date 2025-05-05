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

#include "velox/connectors/hive_common/TableHandle.h"

namespace facebook::velox::connector::hive_common {

namespace {
std::unordered_map<ColumnHandleBase::ColumnType, std::string>
columnTypeNames() {
  return {
      {ColumnHandleBase::ColumnType::kPartitionKey, "PartitionKey"},
      {ColumnHandleBase::ColumnType::kRegular, "Regular"},
      {ColumnHandleBase::ColumnType::kSynthesized, "Synthesized"},
      {ColumnHandleBase::ColumnType::kRowIndex, "RowIndex"},
  };
}

template <typename K, typename V>
std::unordered_map<V, K> invertMap(const std::unordered_map<K, V>& mapping) {
  std::unordered_map<V, K> inverted;
  for (const auto& [key, value] : mapping) {
    inverted.emplace(value, key);
  }
  return inverted;
}

} // namespace

std::string ColumnHandleBase::columnTypeName(
    ColumnHandleBase::ColumnType type) {
  static const auto ctNames = columnTypeNames();
  return ctNames.at(type);
}

ColumnHandleBase::ColumnType ColumnHandleBase::columnTypeFromName(
    const std::string& name) {
  static const auto nameColumnTypes = invertMap(columnTypeNames());
  return nameColumnTypes.at(name);
}

folly::dynamic ColumnHandleBase::serializeBase(std::string_view name) const {
  folly::dynamic obj = ColumnHandle::serializeBase(name);
  obj["columnType"] = columnTypeName(columnType_);
  obj["dataType"] = dataType_->serialize();
  folly::dynamic requiredSubfields = folly::dynamic::array;
  for (const auto& subfield : requiredSubfields_) {
    requiredSubfields.push_back(subfield.toString());
  }
  obj["requiredSubfields"] = requiredSubfields;
  return obj;
}

std::string ColumnHandleBase::toStringBase(const std::string& className) const {
  std::ostringstream out;
  out << fmt::format(
      "{} [columnName: {}, columnType: {}, dataType: {},",
      className,
      columnName_,
      columnTypeName(columnType_),
      dataType_->toString());
  out << " requiredSubfields: [";
  for (const auto& subfield : requiredSubfields_) {
    out << " " << subfield.toString();
  }
  out << " ]]";
  return out.str();
}

ConnectorTableHandlePtr TableHandleBase::create(
    const folly::dynamic& obj,
    void* context) {
  auto connectorId = obj["connectorId"].asString();
  auto tableName = obj["tableName"].asString();
  auto filterPushdownEnabled = obj["filterPushdownEnabled"].asBool();

  core::TypedExprPtr remainingFilter;
  if (auto it = obj.find("remainingFilter"); it != obj.items().end()) {
    remainingFilter =
        ISerializable::deserialize<core::ITypedExpr>(it->second, context);
  }

  velox::common::SubfieldFilters subfieldFilters;
  folly::dynamic subfieldFiltersObj = obj["subfieldFilters"];
  for (const auto& subfieldFilter : subfieldFiltersObj) {
    velox::common::Subfield subfield(subfieldFilter["subfield"].asString());
    auto filter = ISerializable::deserialize<velox::common::Filter>(
        subfieldFilter["filter"]);
    subfieldFilters[velox::common::Subfield(std::move(subfield.path()))] =
        filter->clone();
  }

  RowTypePtr dataColumns;
  if (auto it = obj.find("dataColumns"); it != obj.items().end()) {
    dataColumns = ISerializable::deserialize<RowType>(it->second, context);
  }

  std::unordered_map<std::string, std::string> tableParameters{};
  const auto& tableParametersObj = obj["tableParameters"];
  for (const auto& key : tableParametersObj.keys()) {
    const auto& value = tableParametersObj[key];
    tableParameters.emplace(key.asString(), value.asString());
  }

  return std::make_shared<const TableHandleBase>(
      connectorId,
      tableName,
      filterPushdownEnabled,
      std::move(subfieldFilters),
      remainingFilter,
      dataColumns,
      tableParameters);
}

folly::dynamic TableHandleBase::serializeBase(
    const std::string& className) const {
  folly::dynamic obj = ConnectorTableHandle::serializeBase(className);
  obj["tableName"] = tableName_;
  obj["filterPushdownEnabled"] = filterPushdownEnabled_;

  folly::dynamic subfieldFilters = folly::dynamic::array;
  for (const auto& [subfield, filter] : subfieldFilters_) {
    folly::dynamic pair = folly::dynamic::object;
    pair["subfield"] = subfield.toString();
    pair["filter"] = filter->serialize();
    subfieldFilters.push_back(pair);
  }

  obj["subfieldFilters"] = subfieldFilters;
  if (remainingFilter_) {
    obj["remainingFilter"] = remainingFilter_->serialize();
  }
  if (dataColumns_) {
    obj["dataColumns"] = dataColumns_->serialize();
  }
  folly::dynamic tableParameters = folly::dynamic::object;
  for (const auto& param : tableParameters_) {
    tableParameters[param.first] = param.second;
  }
  obj["tableParameters"] = tableParameters;

  return obj;
}

std::string TableHandleBase::toStringBase(const std::string& className) const {
  std::stringstream out;
  out << className << " [table: " << tableName_;

  if (!subfieldFilters_.empty()) {
    // Sort filters by subfield for deterministic output.
    std::map<std::string, velox::common::Filter*> orderedFilters;
    for (const auto& [field, filter] : subfieldFilters_) {
      orderedFilters[field.toString()] = filter.get();
    }
    out << ", range filters: [";
    bool notFirstFilter = false;
    for (const auto& [field, filter] : orderedFilters) {
      if (notFirstFilter) {
        out << ", ";
      }
      out << "(" << field << ", " << filter->toString() << ")";
      notFirstFilter = true;
    }
    out << "]";
  }
  if (remainingFilter_) {
    out << ", remaining filter: (" << remainingFilter_->toString() << ")";
  }

  if (dataColumns_) {
    out << ", data columns: " << dataColumns_->toString();
  }

  if (!tableParameters_.empty()) {
    std::map<std::string, std::string> orderedTableParameters{
        tableParameters_.begin(), tableParameters_.end()};
    out << ", table parameters: [";
    bool firstParam = true;
    for (const auto& param : orderedTableParameters) {
      if (!firstParam) {
        out << ", ";
      }
      out << param.first << ":" << param.second;
      firstParam = false;
    }
    out << "]";
  }

  out << "]";
  return out.str();
}

// void TableHandleBase::registerSerDe() {
//   auto& registry = DeserializationWithContextRegistryForSharedPtr();
//   registry.Register("ColumnHandleBase", create);
// }

} // namespace facebook::velox::connector::hive_common
