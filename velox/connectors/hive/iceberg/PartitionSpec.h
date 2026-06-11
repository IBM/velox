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

#include "velox/type/Type.h"

namespace facebook::velox::connector::hive::iceberg {

/// Partition transform types.
/// Defines how source column values are converted into partition keys.
/// See https://iceberg.apache.org/spec/#partition-transforms.
enum class TransformType {
  /// Use the source value as-is (no transformation).
  kIdentity,
  /// Extract a timestamp hour, as hours from 1970-01-01 00:00:00.
  kHour,
  /// Extract a date or timestamp day, as days from 1970-01-01.
  kDay,
  /// Extract a date or timestamp month, as months from 1970-01.
  kMonth,
  /// Extract a date or timestamp year, as years from 1970.
  kYear,
  /// Hash the value into N buckets for even distribution. Requires an integer
  /// parameter specifying the bucket count.
  kBucket,
  /// Truncate strings or numbers to a specified width. Requires an integer
  /// parameter specifying the truncate width.
  kTruncate
};

VELOX_DECLARE_ENUM_NAME(TransformType);

struct IcebergPartitionSpec {
  struct Field {
    // The field name of this partition field as it appears in the partition
    // spec. This is the original Iceberg field name, not the transformed name
    // from org.apache.iceberg.PartitionField which includes the transform as a
    // suffix.
    std::string name;

    // The source column type.
    TypePtr type;

    // The transform type applied to the source field (e.g., kIdentity, kBucket,
    // kTruncate, etc.).
    TransformType transformType;

    // Optional parameter for transforms that require configuration
    // (e.g., bucket count or truncate width).
    std::optional<int32_t> parameter;
  };

  const int32_t specId;
  const std::vector<Field> fields;

  IcebergPartitionSpec(int32_t _specId, const std::vector<Field>& _fields)
      : specId(_specId), fields(_fields) {}
};

} // namespace facebook::velox::connector::hive::iceberg
