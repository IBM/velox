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

#include "HivePartitionUtil.h"

#include "HiveConfig.h"
#include "HiveConnectorSplit.h"
#include "velox/dwio/common/CachedBufferedInput.h"
#include "velox/dwio/common/DirectBufferedInput.h"
#include "velox/dwio/common/Reader.h"
#include "velox/expression/Expr.h"
#include "velox/expression/ExprToSubfieldFilter.h"

namespace facebook::velox::connector::lakehouse::hive {

bool applyPartitionFilter(
    const TypePtr& type,
    const std::string& partitionValue,
    common::Filter* filter,
    bool asLocalTime) {
  if (type->isDate()) {
    int32_t value =
        DATE()->toDays(static_cast<folly::StringPiece>(partitionValue));
    return applyFilter(*filter, value);
  }

  switch (type->kind()) {
    case TypeKind::BIGINT:
    case TypeKind::INTEGER:
    case TypeKind::SMALLINT:
    case TypeKind::TINYINT: {
      return applyFilter(*filter, folly::to<int64_t>(partitionValue));
    }
    case TypeKind::REAL:
    case TypeKind::DOUBLE: {
      return applyFilter(*filter, folly::to<double>(partitionValue));
    }
    case TypeKind::BOOLEAN: {
      return applyFilter(*filter, folly::to<bool>(partitionValue));
    }
    case TypeKind::TIMESTAMP: {
      auto result = util::fromTimestampString(
          StringView(partitionValue), util::TimestampParseMode::kPrestoCast);
      VELOX_CHECK(!result.hasError());
      if (asLocalTime) {
        result.value().toGMT(Timestamp::defaultTimezone());
      }
      return applyFilter(*filter, result.value());
    }
    case TypeKind::VARCHAR: {
      return applyFilter(*filter, partitionValue);
    }
    default:
      VELOX_FAIL(
          "Bad type {} for partition value: {}", type->kind(), partitionValue);
  }
}

bool filterSplit(
    const common::ScanSpec* scanSpec,
    const dwio::common::Reader* reader,
    const std::string& filePath,
    const std::unordered_map<std::string, std::optional<std::string>>&
        partitionKeys,
    const std::unordered_map<
        std::string,
        std::shared_ptr<base::ColumnHandleBase>>& partitionKeysHandle,
    bool asLocalTime) {
  const auto totalRows = reader->numberOfRows();
  const auto& fileTypeWithId = reader->typeWithId();
  const auto& rowType = reader->rowType();
  for (const auto& child : scanSpec->children()) {
    if (child->filter()) {
      const auto& name = child->fieldName();
      auto iter = partitionKeys.find(name);
      // By design, the partition key columns for Iceberg tables are included in
      // the data files to facilitate partition transform and partition
      // evolution, so we need to test both cases.
      if (!rowType->containsChild(name) || iter != partitionKeys.end()) {
        if (iter != partitionKeys.end() && iter->second.has_value()) {
          const auto handlesIter = partitionKeysHandle.find(name);
          VELOX_CHECK(handlesIter != partitionKeysHandle.end());

          auto hivePartitionColumnHandle =
              std::dynamic_pointer_cast<HiveColumnHandle>(handlesIter->second);
          VELOX_CHECK_NOT_NULL(hivePartitionColumnHandle);

          // This is a non-null partition key
          return applyPartitionFilter(
              hivePartitionColumnHandle->dataType(),
              iter->second.value(),
              child->filter(),
              asLocalTime);
        }
        // Column is missing, most likely due to schema evolution. Or it's a
        // partition key but the partition value is NULL.
        if (child->filter()->isDeterministic() &&
            !child->filter()->testNull()) {
          VLOG(1) << "Skipping " << filePath
                  << " because the filter testNull() failed for column "
                  << child->fieldName();
          return false;
        }
      } else {
        const auto& typeWithId = fileTypeWithId->childByName(name);
        const auto columnStats = reader->columnStatistics(typeWithId->id());
        if (columnStats != nullptr &&
            !testFilter(
                child->filter(),
                columnStats.get(),
                totalRows.value(),
                typeWithId->type())) {
          VLOG(1) << "Skipping " << filePath
                  << " based on stats and filter for column "
                  << child->fieldName();
          return false;
        }
      }
    }
  }

  return true;
}

} // namespace facebook::velox::connector::lakehouse::hive
