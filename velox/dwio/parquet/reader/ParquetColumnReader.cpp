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

//
// Created by Ying Su on 2/14/22.
//

#include "velox/dwio/parquet/reader/ParquetColumnReader.h"

#include "velox/dwio/common/SelectiveColumnReaderInternal.h"
#include "velox/dwio/parquet/reader/BooleanColumnReader.h"
#include "velox/dwio/parquet/reader/FloatingPointColumnReader.h"
#include "velox/dwio/parquet/reader/IntegerColumnReader.h"
#include "velox/dwio/parquet/reader/RepeatedColumnReader.h"
#include "velox/dwio/parquet/reader/StringColumnReader.h"
#include "velox/dwio/parquet/reader/StructColumnReader.h"
#include "velox/dwio/parquet/reader/TimestampColumnReader.h"
#include "velox/dwio/parquet/thrift/ParquetThriftTypes.h"

namespace facebook::velox::parquet {

// static
std::unique_ptr<dwio::common::SelectiveColumnReader> ParquetColumnReader::build(
    const dwio::common::ColumnReaderOptions& columnReaderOptions,
    const TypePtr& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    ParquetParams& params,
    common::ScanSpec& scanSpec,
    memory::MemoryPool& pool) {
  auto colName = scanSpec.fieldName();

  switch (fileType->type()->kind()) {
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::SMALLINT:
    case TypeKind::TINYINT:
    case TypeKind::HUGEINT:
      return std::make_unique<IntegerColumnReader>(
          requestedType, fileType, params, scanSpec);

    case TypeKind::REAL:
      return std::make_unique<FloatingPointColumnReader<float, float>>(
          requestedType, fileType, params, scanSpec);
    case TypeKind::DOUBLE:
      return std::make_unique<FloatingPointColumnReader<double, double>>(
          requestedType, fileType, params, scanSpec);

    case TypeKind::ROW:
      return std::make_unique<StructColumnReader>(
          columnReaderOptions, requestedType, fileType, params, scanSpec, pool);

    case TypeKind::VARBINARY:
    case TypeKind::VARCHAR:
      return std::make_unique<StringColumnReader>(fileType, params, scanSpec);

    case TypeKind::ARRAY:
      return std::make_unique<ListColumnReader>(
          columnReaderOptions, requestedType, fileType, params, scanSpec, pool);

    case TypeKind::MAP:
      return std::make_unique<MapColumnReader>(
          columnReaderOptions, requestedType, fileType, params, scanSpec, pool);

    case TypeKind::BOOLEAN:
      return std::make_unique<BooleanColumnReader>(
          requestedType, fileType, params, scanSpec);

    case TypeKind::TIMESTAMP: {
      const auto parquetType =
          std::static_pointer_cast<const ParquetTypeWithId>(fileType)
              ->parquetType_;
      VELOX_CHECK(parquetType);
      switch (parquetType.value()) {
        case thrift::Type::INT64:
          return std::make_unique<TimestampColumnReader<int64_t>>(
              requestedType, fileType, params, scanSpec);
        case thrift::Type::INT96:
          return std::make_unique<TimestampColumnReader<int128_t>>(
              requestedType, fileType, params, scanSpec);
        default:
          VELOX_UNREACHABLE();
      }
    }

    default:
      VELOX_FAIL(
          "buildReader unhandled type: " +
          mapTypeKindToName(fileType->type()->kind()));
  }
}

} // namespace facebook::velox::parquet
