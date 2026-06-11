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

#include "velox/connectors/hive/TableHandle.h"
#include "velox/dwio/common/ParquetFieldId.h"

namespace facebook::velox::connector::hive::iceberg {

struct IcebergNestedField {
  int32_t id;
  std::vector<IcebergNestedField> children;

  /// Converts this IcebergNestedField to a ParquetFieldId recursively.
  dwio::common::ParquetFieldId toParquetFieldId() const {
    dwio::common::ParquetFieldId parquetField;
    parquetField.fieldId = id;
    parquetField.children.reserve(children.size());
    for (const auto& child : children) {
      parquetField.children.push_back(child.toParquetFieldId());
    }
    return parquetField;
  }

  /// Creates an IcebergNestedField from a ParquetFieldId recursively.
  static IcebergNestedField fromParquetFieldId(
      const dwio::common::ParquetFieldId& parquetField) {
    IcebergNestedField field;
    field.id = parquetField.fieldId;
    field.children.reserve(parquetField.children.size());
    for (const auto& child : parquetField.children) {
      field.children.push_back(fromParquetFieldId(child));
    }
    return field;
  }
};

class IcebergColumnHandle : public HiveColumnHandle {
 public:
  IcebergColumnHandle(
      const std::string& name,
      ColumnType columnType,
      TypePtr dataType,
      TypePtr hiveType,
      const IcebergNestedField& nestedField,
      std::vector<common::Subfield> requiredSubfields = {},
      std::optional<std::string> initialDefaultValue = std::nullopt,
      ColumnParseParameters columnParseParameters = {});

  // Constructor overload that accepts ParquetFieldId for convenience
  IcebergColumnHandle(
      const std::string& name,
      ColumnType columnType,
      TypePtr dataType,
      const dwio::common::ParquetFieldId& parquetField,
      std::vector<common::Subfield> requiredSubfields = {},
      std::optional<std::string> initialDefaultValue = std::nullopt,
      ColumnParseParameters columnParseParameters = {})
      : IcebergColumnHandle(
            name,
            columnType,
            dataType,
            dataType,
            IcebergNestedField::fromParquetFieldId(parquetField),
            std::move(requiredSubfields),
            std::move(initialDefaultValue),
            columnParseParameters) {}

  const IcebergNestedField& nestedField() const;

  const std::optional<std::string>& initialDefaultValue() const {
    return initialDefaultValue_;
  }

 private:
  const IcebergNestedField nestedField_;
  const std::optional<std::string> initialDefaultValue_;
};

using IcebergColumnHandlePtr = std::shared_ptr<const IcebergColumnHandle>;

} // namespace facebook::velox::connector::hive::iceberg
