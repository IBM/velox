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

#include "ConnectorConfigBase.h"
#include "ConnectorSplitBase.h"
#include "FileHandle.h"
#include "TableHandleBase.h"
#include "velox/core/Expressions.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/type/Type.h"

#include <string>

namespace facebook::velox::connector::lakehouse::base {

struct SubfieldSpec {
  const common::Subfield* subfield;
  bool filterOnly;
};

bool isSynthesizedColumn(
    const std::string& name,
    const std::unordered_map<std::string, std::shared_ptr<ColumnHandleBase>>&
        infoColumns);

bool isSpecialColumn(
    const std::string& name,
    const std::optional<std::string>& specialName);

void processFieldSpec(
    const RowTypePtr& dataColumns,
    const TypePtr& outputType,
    common::ScanSpec& fieldSpec);

const std::string& getColumnName(
    const common::Subfield& subfield);

void checkColumnNameLowerCase(
    const std::shared_ptr<const Type>& type);

void checkColumnNameLowerCase(
    const common::SubfieldFilters& filters,
    const std::unordered_map<
        std::string,
        std::shared_ptr<ColumnHandleBase>>&
        infoColumns);

void checkColumnNameLowerCase(
    const core::TypedExprPtr& typeExpr);

void configureReaderOptions(
    const std::shared_ptr<
        const ConnectorConfigBase>&
        ConnectorConfigBase,
    const connector::ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<
        const TableHandleBase>& tableHandle,
    const std::shared_ptr<
        const ConnectorSplitBase>& split,
    dwio::common::ReaderOptions& readerOptions);

void configureReaderOptions(
    const std::shared_ptr<
        const ConnectorConfigBase>&
        ConnectorConfigBase,
    const connector::ConnectorQueryCtx* connectorQueryCtx,
    const RowTypePtr& fileSchema,
    const std::shared_ptr<
        const ConnectorSplitBase>& split,
    const std::unordered_map<std::string, std::string>& tableParameters,
    dwio::common::ReaderOptions& readerOptions);

void configureRowReaderOptions(
    const std::unordered_map<std::string, std::string>& tableParameters,
    const std::shared_ptr<common::ScanSpec>& scanSpec,
    std::shared_ptr<common::MetadataFilter> metadataFilter,
    const RowTypePtr& rowType,
    const std::shared_ptr<
        const ConnectorSplitBase>&
        hiveSplit,
    const std::shared_ptr<
        const ConnectorConfigBase>& hiveConfig,
    const config::ConfigBase* sessionProperties,
    dwio::common::RowReaderOptions& rowReaderOptions);

void configureRowReaderOptions(
    const std::unordered_map<std::string, std::string>& tableParameters,
    const std::shared_ptr<common::ScanSpec>& scanSpec,
    std::shared_ptr<common::MetadataFilter> metadataFilter,
    const RowTypePtr& rowType,
    const std::shared_ptr<
        const ConnectorSplitBase>&
        hiveSplit,
    const std::shared_ptr<
        const ConnectorConfigBase>& hiveConfig,
    const config::ConfigBase* sessionProperties,
    dwio::common::RowReaderOptions& rowReaderOptions);

void addSubfields(
    const Type& type,
    std::vector<SubfieldSpec>& subfields,
    int level,
    memory::MemoryPool* pool,
    common::ScanSpec& spec);

template <TypeKind kind>
VectorPtr newConstantFromString(
    const TypePtr& type,
    const std::optional<std::string>& value,
    vector_size_t size,
    velox::memory::MemoryPool* pool,
    const std::string& sessionTimezone,
    bool asLocalTime,
    bool isPartitionDateDaysSinceEpoch = false){
  using T = typename TypeTraits<kind>::NativeType;
  if (!value.has_value()) {
    return std::make_shared<ConstantVector<T>>(pool, size, true, type, T());
  }

  if (type->isDate()) {
    int32_t days = 0;
    // For Iceberg, the date partition values are already in daysSinceEpoch
    // form.
    if (isPartitionDateDaysSinceEpoch) {
      days = folly::to<int32_t>(value.value());
    } else {
      days = DATE()->toDays(static_cast<folly::StringPiece>(value.value()));
    }
    return std::make_shared<ConstantVector<int32_t>>(
        pool, size, false, type, std::move(days));
  }

  if constexpr (std::is_same_v<T, StringView>) {
    return std::make_shared<ConstantVector<StringView>>(
        pool, size, false, type, StringView(value.value()));
  } else {
    auto copy = velox::util::Converter<kind>::tryCast(value.value())
                    .thenOrThrow(folly::identity, [&](const Status& status) {
                      VELOX_USER_FAIL("{}", status.message());
                    });
    if constexpr (kind == TypeKind::TIMESTAMP) {
      if (asLocalTime) {
        copy.toGMT(Timestamp::defaultTimezone());
      }
    }
    return std::make_shared<ConstantVector<T>>(
        pool, size, false, type, std::move(copy));
  }
}

std::unique_ptr<dwio::common::BufferedInput>
createBufferedInput(
    const FileHandle& fileHandle,
    const dwio::common::ReaderOptions& readerOpts,
    const connector::ConnectorQueryCtx* connectorQueryCtx,
    std::shared_ptr<io::IoStatistics> ioStats,
    std::shared_ptr<filesystems::File::IoStats> fsStats,
    folly::Executor* executor);

core::TypedExprPtr extractFiltersFromRemainingFilter(
    const core::TypedExprPtr& expr,
    core::ExpressionEvaluator* evaluator,
    bool negated,
    common::SubfieldFilters& filters,
    double& sampleRate);

} // namespace connector::common
