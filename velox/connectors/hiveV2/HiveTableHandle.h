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

#include "velox/connectors/Connector.h"
#include "velox/connectors/hive_common/TableHandle.h"
#include "velox/core/ITypedExpr.h"
#include "velox/type/Filter.h"
#include "velox/type/Subfield.h"
#include "velox/type/Type.h"

#include <string>

namespace facebook::velox::connector::hiveV2 {

class HiveColumnHandle : public hive_common::ColumnHandleBase {
 public:
  /// NOTE: 'dataType' is the column type in target write table. 'hiveType' is
  /// converted type of the corresponding column in source table which might not
  /// be the same type, and the table scan needs to do data coercion if needs.
  /// The table writer also needs to respect the type difference when processing
  /// input data such as bucket id calculation.
  HiveColumnHandle(
      const std::string& name,
      ColumnType columnType,
      TypePtr dataType,
      TypePtr hiveType,
      std::vector<velox::common::Subfield> requiredSubfields = {},
      ColumnParseParameters columnParseParameters = {})
      : ColumnHandleBase(
            name,
            columnType,
            std::move(dataType),
            std::move(requiredSubfields),
            columnParseParameters),
        hiveType_(std::move(hiveType)) {
    VELOX_USER_CHECK(
        dataType_->equivalent(*hiveType_),
        "data type {} and hive type {} do not match",
        dataType_->toString(),
        hiveType_->toString());
  }

  const TypePtr& hiveType() const {
    return hiveType_;
  }

  bool isPartitionDateValueDaysSinceEpoch() const {
    return columnParseParameters_.partitionDateValueFormat ==
        ColumnParseParameters::kDaysSinceEpoch;
  }

  virtual folly::dynamic serialize() const override;

  virtual std::string toString() const override;

  static ColumnHandlePtr create(const folly::dynamic& obj);

  static void registerSerDe();

 private:
  const TypePtr hiveType_;
};

class HiveTableHandle : public hive_common::TableHandleBase {
 public:
  HiveTableHandle(
      std::string connectorId,
      const std::string& tableName,
      bool filterPushdownEnabled,
      velox::common::SubfieldFilters subfieldFilters,
      const core::TypedExprPtr& remainingFilter,
      const RowTypePtr& dataColumns = nullptr,
      const std::unordered_map<std::string, std::string>& tableParameters = {})
      : TableHandleBase(
            std::move(connectorId),
            tableName,
            filterPushdownEnabled,
            std::move(subfieldFilters),
            remainingFilter,
            dataColumns,
            tableParameters) {}

  std::string toString() const override;

  folly::dynamic serialize() const override;

  static ConnectorTableHandlePtr create(
      const folly::dynamic& obj,
      void* context);

  static void registerSerDe();
};

} // namespace facebook::velox::connector::hiveV2
