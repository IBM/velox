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

#include "velox/connectors/lakehouse/base/TableHandleBase.h"

#include <string>

namespace facebook::velox::connector::lakehouse::iceberg  {

class IcebergColumnHandle : public base::ColumnHandleBase {
 public:
  /// NOTE: 'dataType' is the column type in target write table. 'hiveType' is
  /// converted type of the corresponding column in source table which might not
  /// be the same type, and the table scan needs to do data coercion if needs.
  /// The table writer also needs to respect the type difference when processing
  /// input data such as bucket id calculation.
  IcebergColumnHandle(
      const std::string& name,
      ColumnType columnType,
      TypePtr dataType,
      std::vector<velox::common::Subfield> requiredSubfields = {})
      : base::ColumnHandleBase(
            name,
            columnType,
            std::move(dataType),
            std::move(requiredSubfields)) {}

  virtual folly::dynamic serialize() const override;

  virtual std::string toString() const override;

  static ColumnHandlePtr create(const folly::dynamic& obj);

  static void registerSerDe();

  bool isPartitionKey() {
    VELOX_NYI();
  }
};

class IcebergTableHandle : public base::TableHandleBase {
 public:
  IcebergTableHandle(
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

} // namespace facebook::velox::connector::lakehouse::iceberg
