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

#include "velox/connectors/hive_common/ConnectorUtil.h"

#include "velox/dwio/common/CachedBufferedInput.h"
#include "velox/dwio/common/DirectBufferedInput.h"
#include "velox/dwio/common/Reader.h"
#include "velox/expression/Expr.h"
#include "velox/expression/ExprToSubfieldFilter.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ConstantVector.h"

using namespace facebook::velox;

namespace facebook::velox::connector::hive_common {

using namespace facebook::velox::common;

namespace {

core::CallTypedExprPtr replaceInputs(
    const core::CallTypedExpr* call,
    std::vector<core::TypedExprPtr>&& inputs) {
  return std::make_shared<core::CallTypedExpr>(
      call->type(), std::move(inputs), call->name());
}

bool endWith(const std::string& str, const char* suffix) {
  int len = strlen(suffix);
  if (str.size() < len) {
    return false;
  }
  for (int i = 0, j = str.size() - len; i < len; ++i, ++j) {
    if (str[j] != suffix[i]) {
      return false;
    }
  }
  return true;
}

bool isNotExpr(
    const core::TypedExprPtr& expr,
    const core::CallTypedExpr* call,
    core::ExpressionEvaluator* evaluator) {
  if (!endWith(call->name(), "not")) {
    return false;
  }
  auto exprs = evaluator->compile(expr);
  VELOX_CHECK_EQ(exprs->size(), 1);
  auto& compiled = exprs->expr(0);
  return compiled->vectorFunction() &&
      compiled->vectorFunction()->getCanonicalName() ==
      exec::FunctionCanonicalName::kNot;
}

double getPrestoSampleRate(
    const core::TypedExprPtr& expr,
    const core::CallTypedExpr* call,
    core::ExpressionEvaluator* evaluator) {
  if (!endWith(call->name(), "lt")) {
    return -1;
  }
  VELOX_CHECK_EQ(call->inputs().size(), 2);
  auto exprs = evaluator->compile(expr);
  VELOX_CHECK_EQ(exprs->size(), 1);
  auto& lt = exprs->expr(0);
  if (!(lt->vectorFunction() &&
        lt->vectorFunction()->getCanonicalName() ==
            exec::FunctionCanonicalName::kLt)) {
    return -1;
  }
  auto& rand = lt->inputs()[0];
  if (!(rand->inputs().empty() && rand->vectorFunction() &&
        rand->vectorFunction()->getCanonicalName() ==
            exec::FunctionCanonicalName::kRand)) {
    return -1;
  }
  auto* rate =
      dynamic_cast<const core::ConstantTypedExpr*>(call->inputs()[1].get());
  if (!(rate && rate->type()->kind() == TypeKind::DOUBLE)) {
    return -1;
  }
  return std::max(0.0, std::min(1.0, rate->value().value<double>()));
}

template <typename T>
void deduplicate(std::vector<T>& values) {
  std::sort(values.begin(), values.end());
  values.erase(std::unique(values.begin(), values.end()), values.end());
}

// Floating point map key subscripts are truncated toward 0 in Presto.  For
// example given `a' as a map with floating point key, if user queries a[0.99],
// Presto coordinator will generate a required subfield a[0]; for a[-1.99] it
// will generate a[-1]; for anything larger than 9223372036854775807, it
// generates a[9223372036854775807]; for anything smaller than
// -9223372036854775808 it generates a[-9223372036854775808].
template <typename T>
std::unique_ptr<velox::common::Filter> makeFloatingPointMapKeyFilter(
    const std::vector<int64_t>& subscripts) {
  std::vector<std::unique_ptr<velox::common::Filter>> filters;
  for (auto subscript : subscripts) {
    T lower = subscript;
    T upper = subscript;
    bool lowerUnbounded = subscript == std::numeric_limits<int64_t>::min();
    bool upperUnbounded = subscript == std::numeric_limits<int64_t>::max();
    bool lowerExclusive = false;
    bool upperExclusive = false;
    if (lower <= 0 && !lowerUnbounded) {
      if (lower > subscript - 1) {
        lower = subscript - 1;
      } else {
        lower = std::nextafter(lower, -std::numeric_limits<T>::infinity());
      }
      lowerExclusive = true;
    }
    if (upper >= 0 && !upperUnbounded) {
      if (upper < subscript + 1) {
        upper = subscript + 1;
      } else {
        upper = std::nextafter(upper, std::numeric_limits<T>::infinity());
      }
      upperExclusive = true;
    }
    if (lowerUnbounded && upperUnbounded) {
      continue;
    }
    filters.push_back(
        std::make_unique<velox::common::FloatingPointRange<T>>(
            lower,
            lowerUnbounded,
            lowerExclusive,
            upper,
            upperUnbounded,
            upperExclusive,
            false));
  }
  if (filters.size() == 1) {
    return std::move(filters[0]);
  }
  return std::make_unique<velox::common::MultiRange>(std::move(filters), false);
}

inline uint8_t parseDelimiter(const std::string& delim) {
  for (char const& ch : delim) {
    if (!std::isdigit(ch)) {
      return delim[0];
    }
  }
  return stoi(delim);
}

std::unique_ptr<dwio::common::SerDeOptions> parseSerdeParameters(
    const std::unordered_map<std::string, std::string>& serdeParameters,
    const std::unordered_map<std::string, std::string>& tableParameters) {
  auto fieldIt = serdeParameters.find(dwio::common::SerDeOptions::kFieldDelim);
  if (fieldIt == serdeParameters.end()) {
    fieldIt = serdeParameters.find("serialization.format");
  }
  auto collectionIt =
      serdeParameters.find(dwio::common::SerDeOptions::kCollectionDelim);
  if (collectionIt == serdeParameters.end()) {
    // For collection delimiter, Hive 1.x, 2.x uses "colelction.delim", but
    // Hive 3.x uses "collection.delim".
    // See: https://issues.apache.org/jira/browse/HIVE-16922)
    collectionIt = serdeParameters.find("colelction.delim");
  }
  auto mapKeyIt =
      serdeParameters.find(dwio::common::SerDeOptions::kMapKeyDelim);

  auto escapeCharIt =
      serdeParameters.find(dwio::common::SerDeOptions::kEscapeChar);

  auto nullStringIt = tableParameters.find(
      dwio::common::TableParameter::kSerializationNullFormat);

  if (fieldIt == serdeParameters.end() &&
      collectionIt == serdeParameters.end() &&
      mapKeyIt == serdeParameters.end() &&
      escapeCharIt == serdeParameters.end() &&
      nullStringIt == tableParameters.end()) {
    return nullptr;
  }

  uint8_t fieldDelim = '\1';
  uint8_t collectionDelim = '\2';
  uint8_t mapKeyDelim = '\3';
  if (fieldIt != serdeParameters.end()) {
    fieldDelim = parseDelimiter(fieldIt->second);
  }
  if (collectionIt != serdeParameters.end()) {
    collectionDelim = parseDelimiter(collectionIt->second);
  }
  if (mapKeyIt != serdeParameters.end()) {
    mapKeyDelim = parseDelimiter(mapKeyIt->second);
  }

  // If escape character is specified then we use it, unless it is empty - in
  // which case we default to '\\'.
  // If escape character is not specified (not in the map) we turn escaping off.
  // Logic is based on apache hive java code:
  // https://github.com/apache/hive/blob/3f6f940af3f60cc28834268e5d7f5612e3b13c30/serde/src/java/org/apache/hadoop/hive/serde2/lazy/LazySerDeParameters.java#L105-L108
  uint8_t escapeChar = '\\';
  const bool hasEscapeChar = (escapeCharIt != serdeParameters.end());
  if (hasEscapeChar) {
    if (!escapeCharIt->second.empty()) {
      // If delim is convertible to uint8_t then we use it as character code,
      // otherwise we use the 1st character of the string.
      escapeChar = folly::tryTo<uint8_t>(escapeCharIt->second)
                       .value_or(escapeCharIt->second[0]);
    }
  }

  auto serDeOptions = hasEscapeChar
      ? std::make_unique<dwio::common::SerDeOptions>(
            fieldDelim, collectionDelim, mapKeyDelim, escapeChar, true)
      : std::make_unique<dwio::common::SerDeOptions>(
            fieldDelim, collectionDelim, mapKeyDelim);
  if (nullStringIt != tableParameters.end()) {
    serDeOptions->nullString = nullStringIt->second;
  }
  return serDeOptions;
}

} // namespace

bool isSynthesizedColumn(
    const std::string& name,
    const std::unordered_map<std::string, std::shared_ptr<ColumnHandleBase>>&
        infoColumns) {
  return infoColumns.count(name) != 0;
}

bool isSpecialColumn(
    const std::string& name,
    const std::optional<std::string>& specialName) {
  return specialName.has_value() && name == *specialName;
}

void processFieldSpec(
    const RowTypePtr& dataColumns,
    const TypePtr& outputType,
    velox::common::ScanSpec& fieldSpec) {
  fieldSpec.visit(
      *outputType, [](const Type& type, velox::common::ScanSpec& spec) {
        if (type.isMap() && !spec.isConstant()) {
          auto* keys =
              spec.childByName(velox::common::ScanSpec::kMapKeysFieldName);
          VELOX_CHECK_NOT_NULL(keys);
          keys->addFilter(velox::common::IsNotNull());
        }
      });
  if (dataColumns) {
    auto i = dataColumns->getChildIdxIfExists(fieldSpec.fieldName());
    if (i.has_value()) {
      if (dataColumns->childAt(*i)->isMap() && outputType->isRow()) {
        fieldSpec.setFlatMapAsStruct(true);
      }
    }
  }
}

const std::string& getColumnName(const velox::common::Subfield& subfield) {
  VELOX_CHECK_GT(subfield.path().size(), 0);
  auto* field =
      dynamic_cast<const Subfield::NestedField*>(subfield.path()[0].get());
  VELOX_CHECK_NOT_NULL(field);
  return field->name();
}

void checkColumnNameLowerCase(const std::shared_ptr<const Type>& type) {
  switch (type->kind()) {
    case TypeKind::ARRAY:
      checkColumnNameLowerCase(type->asArray().elementType());
      break;
    case TypeKind::MAP: {
      checkColumnNameLowerCase(type->asMap().keyType());
      checkColumnNameLowerCase(type->asMap().valueType());

    } break;
    case TypeKind::ROW: {
      for (const auto& outputName : type->asRow().names()) {
        VELOX_CHECK(
            !std::any_of(outputName.begin(), outputName.end(), isupper));
      }
      for (auto& childType : type->asRow().children()) {
        checkColumnNameLowerCase(childType);
      }
    } break;
    default:
      VLOG(1) << "No need to check type lowercase mode" << type->toString();
  }
}

void checkColumnNameLowerCase(
    const velox::common::SubfieldFilters& filters,
    const std::unordered_map<std::string, std::shared_ptr<ColumnHandleBase>>&
        infoColumns) {
  for (const auto& filterIt : filters) {
    const auto name = filterIt.first.toString();
    if (isSynthesizedColumn(name, infoColumns)) {
      continue;
    }
    const auto& path = filterIt.first.path();

    for (int i = 0; i < path.size(); ++i) {
      auto* nestedField =
          dynamic_cast<const velox::common::Subfield::NestedField*>(
              path[i].get());
      if (nestedField == nullptr) {
        continue;
      }
      VELOX_CHECK(!std::any_of(
          nestedField->name().begin(), nestedField->name().end(), isupper));
    }
  }
}

void checkColumnNameLowerCase(const core::TypedExprPtr& typeExpr) {
  if (typeExpr == nullptr) {
    return;
  }
  checkColumnNameLowerCase(typeExpr->type());
  for (auto& type : typeExpr->inputs()) {
    checkColumnNameLowerCase(type);
  }
}

void configureReaderOptions(
    const std::shared_ptr<const ConnectorConfig>& connectorConfig,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<const TableHandleBase>& tableHandle,
    const std::shared_ptr<const ConnectorSplitBase>& split,
    dwio::common::ReaderOptions& readerOptions) {
  configureReaderOptions(
      connectorConfig,
      connectorQueryCtx,
      tableHandle->dataColumns(),
      split,
      tableHandle->tableParameters(),
      readerOptions);
}

void configureReaderOptions(
    const std::shared_ptr<const ConnectorConfig>& connectorConfig,
    const ConnectorQueryCtx* connectorQueryCtx,
    const RowTypePtr& fileSchema,
    const std::shared_ptr<const ConnectorSplitBase>& split,
    const std::unordered_map<std::string, std::string>& tableParameters,
    dwio::common::ReaderOptions& readerOptions) {
  auto sessionProperties = connectorQueryCtx->sessionProperties();

  readerOptions.setLoadQuantum(connectorConfig->loadQuantum(sessionProperties));
  readerOptions.setMaxCoalesceBytes(
      connectorConfig->maxCoalescedBytes(sessionProperties));
  readerOptions.setMaxCoalesceDistance(
      connectorConfig->maxCoalescedDistanceBytes(sessionProperties));
  readerOptions.setFileColumnNamesReadAsLowerCase(
      connectorConfig->isFileColumnNamesReadAsLowerCase(sessionProperties));

  bool useColumnNamesForColumnMapping = false;
  switch (split->fileFormat) {
    case dwio::common::FileFormat::DWRF:
    case dwio::common::FileFormat::ORC: {
      useColumnNamesForColumnMapping =
          connectorConfig->isOrcUseColumnNames(sessionProperties);
      break;
    }
    case dwio::common::FileFormat::PARQUET: {
      useColumnNamesForColumnMapping =
          connectorConfig->isParquetUseColumnNames(sessionProperties);
      break;
    }
    default:
      useColumnNamesForColumnMapping = false;
  }
  readerOptions.setUseColumnNamesForColumnMapping(
      useColumnNamesForColumnMapping);

  readerOptions.setFileSchema(fileSchema);
  readerOptions.setFooterEstimatedSize(connectorConfig->footerEstimatedSize());
  readerOptions.setFilePreloadThreshold(
      connectorConfig->filePreloadThreshold());
  readerOptions.setPrefetchRowGroups(connectorConfig->prefetchRowGroups());
  readerOptions.setNoCacheRetention(!split->cacheable);

  const auto& sessionTzName = connectorQueryCtx->sessionTimezone();
  if (!sessionTzName.empty()) {
    const auto timezone = tz::locateZone(sessionTzName);
    readerOptions.setSessionTimezone(timezone);
  }
  readerOptions.setAdjustTimestampToTimezone(
      connectorQueryCtx->adjustTimestampToTimezone());
  readerOptions.setSelectiveNimbleReaderEnabled(
      connectorQueryCtx->selectiveNimbleReaderEnabled());

  if (readerOptions.fileFormat() != dwio::common::FileFormat::UNKNOWN) {
    VELOX_CHECK(
        readerOptions.fileFormat() == split->fileFormat,
        "DataSource received splits of different formats: {} and {}",
        dwio::common::toString(readerOptions.fileFormat()),
        dwio::common::toString(split->fileFormat));
  } else {
    auto serDeOptions =
        parseSerdeParameters(split->serdeParameters, tableParameters);
    if (serDeOptions) {
      readerOptions.setSerDeOptions(*serDeOptions);
    }

    readerOptions.setFileFormat(split->fileFormat);
  }
}

void configureRowReaderOptions(
    const std::unordered_map<std::string, std::string>& tableParameters,
    const std::shared_ptr<velox::common::ScanSpec>& scanSpec,
    std::shared_ptr<velox::common::MetadataFilter> metadataFilter,
    const RowTypePtr& rowType,
    const std::shared_ptr<const ConnectorSplitBase>& split,
    const std::shared_ptr<const ConnectorConfig>& connectorConfig,
    const config::ConfigBase* sessionProperties,
    dwio::common::RowReaderOptions& rowReaderOptions) {
  auto skipRowsIt =
      tableParameters.find(dwio::common::TableParameter::kSkipHeaderLineCount);
  if (skipRowsIt != tableParameters.end()) {
    rowReaderOptions.setSkipRows(folly::to<uint64_t>(skipRowsIt->second));
  }
  rowReaderOptions.setScanSpec(scanSpec);
  rowReaderOptions.setMetadataFilter(std::move(metadataFilter));
  rowReaderOptions.setRequestedType(rowType);
  rowReaderOptions.range(split->start, split->length);
  if (connectorConfig && sessionProperties) {
    rowReaderOptions.setTimestampPrecision(
        static_cast<TimestampPrecision>(
            connectorConfig->readTimestampUnit(sessionProperties)));
  }
}

// Recursively add subfields to scan spec.
void addSubfields(
    const Type& type,
    std::vector<SubfieldSpec>& subfields,
    int level,
    memory::MemoryPool* pool,
    velox::common::ScanSpec& spec) {
  int newSize = 0;
  for (int i = 0; i < subfields.size(); ++i) {
    if (level < subfields[i].subfield->path().size()) {
      subfields[newSize++] = subfields[i];
    } else if (!subfields[i].filterOnly) {
      spec.addAllChildFields(type);
      return;
    }
  }
  subfields.resize(newSize);
  switch (type.kind()) {
    case TypeKind::ROW: {
      folly::F14FastMap<std::string, std::vector<SubfieldSpec>> required;
      for (auto& subfield : subfields) {
        auto* element = subfield.subfield->path()[level].get();
        auto* nestedField =
            dynamic_cast<const velox::common::Subfield::NestedField*>(element);
        VELOX_CHECK(
            nestedField,
            "Unsupported for row subfields pruning: {}",
            element->toString());
        required[nestedField->name()].push_back(subfield);
      }
      auto& rowType = type.asRow();
      for (int i = 0; i < rowType.size(); ++i) {
        auto& childName = rowType.nameOf(i);
        auto& childType = rowType.childAt(i);
        auto* child = spec.addField(childName, i);
        auto it = required.find(childName);
        if (it == required.end()) {
          child->setConstantValue(
              BaseVector::createNullConstant(childType, 1, pool));
        } else {
          addSubfields(*childType, it->second, level + 1, pool, *child);
        }
      }
      break;
    }
    case TypeKind::MAP: {
      auto& keyType = type.childAt(0);
      auto* keys = spec.addMapKeyFieldRecursively(*keyType);
      addSubfields(
          *type.childAt(1),
          subfields,
          level + 1,
          pool,
          *spec.addMapValueField());
      if (subfields.empty()) {
        return;
      }
      bool stringKey = keyType->isVarchar() || keyType->isVarbinary();
      std::vector<std::string> stringSubscripts;
      std::vector<int64_t> longSubscripts;
      for (auto& subfield : subfields) {
        auto* element = subfield.subfield->path()[level].get();
        if (dynamic_cast<const velox::common::Subfield::AllSubscripts*>(
                element)) {
          return;
        }
        if (stringKey) {
          auto* subscript =
              dynamic_cast<const velox::common::Subfield::StringSubscript*>(
                  element);
          VELOX_CHECK(
              subscript,
              "Unsupported for string map pruning: {}",
              element->toString());
          stringSubscripts.push_back(subscript->index());
        } else {
          auto* subscript =
              dynamic_cast<const velox::common::Subfield::LongSubscript*>(
                  element);
          VELOX_CHECK(
              subscript,
              "Unsupported for long map pruning: {}",
              element->toString());
          longSubscripts.push_back(subscript->index());
        }
      }
      std::unique_ptr<velox::common::Filter> filter;
      if (stringKey) {
        deduplicate(stringSubscripts);
        filter = std::make_unique<velox::common::BytesValues>(
            stringSubscripts, false);
        spec.setFlatMapFeatureSelection(std::move(stringSubscripts));
      } else {
        deduplicate(longSubscripts);
        if (keyType->isReal()) {
          filter = makeFloatingPointMapKeyFilter<float>(longSubscripts);
        } else if (keyType->isDouble()) {
          filter = makeFloatingPointMapKeyFilter<double>(longSubscripts);
        } else {
          filter = velox::common::createBigintValues(longSubscripts, false);
        }
        std::vector<std::string> features;
        for (auto num : longSubscripts) {
          features.push_back(std::to_string(num));
        }
        spec.setFlatMapFeatureSelection(std::move(features));
      }
      keys->setFilter(std::move(filter));
      break;
    }
    case TypeKind::ARRAY: {
      addSubfields(
          *type.childAt(0),
          subfields,
          level + 1,
          pool,
          *spec.addArrayElementField());
      if (subfields.empty()) {
        return;
      }
      constexpr long kMaxIndex = std::numeric_limits<vector_size_t>::max();
      long maxIndex = -1;
      for (auto& subfield : subfields) {
        auto* element = subfield.subfield->path()[level].get();
        if (dynamic_cast<const velox::common::Subfield::AllSubscripts*>(
                element)) {
          return;
        }
        auto* subscript =
            dynamic_cast<const velox::common::Subfield::LongSubscript*>(
                element);
        VELOX_CHECK(
            subscript,
            "Unsupported for array pruning: {}",
            element->toString());
        VELOX_USER_CHECK_GT(
            subscript->index(),
            0,
            "Non-positive array subscript cannot be push down");
        maxIndex = std::max(maxIndex, std::min(kMaxIndex, subscript->index()));
      }
      spec.setMaxArrayElementsCount(maxIndex);
      break;
    }
    default:
      break;
  }
}

//template <TypeKind kind>
//VectorPtr newConstantFromString(
//    const TypePtr& type,
//    const std::optional<std::string>& value,
//    vector_size_t size,
//    velox::memory::MemoryPool* pool,
//    const std::string& sessionTimezone,
//    bool asLocalTime,
//    bool isPartitionDateDaysSinceEpoch) {
//  using T = typename TypeTraits<kind>::NativeType;
//  if (!value.has_value()) {
//    return std::make_shared<ConstantVector<T>>(pool, size, true, type, T());
//  }
//
//  if (type->isDate()) {
//    int32_t days = 0;
//    // For Iceberg, the date partition values are already in daysSinceEpoch
//    // form.
//    if (isPartitionDateDaysSinceEpoch) {
//      days = folly::to<int32_t>(value.value());
//    } else {
//      days = DATE()->toDays(static_cast<folly::StringPiece>(value.value()));
//    }
//    return std::make_shared<ConstantVector<int32_t>>(
//        pool, size, false, type, std::move(days));
//  }
//
//  if constexpr (std::is_same_v<T, StringView>) {
//    return std::make_shared<ConstantVector<StringView>>(
//        pool, size, false, type, StringView(value.value()));
//  } else {
//    auto copy = velox::util::Converter<kind>::tryCast(value.value())
//                    .thenOrThrow(folly::identity, [&](const Status& status) {
//                      VELOX_USER_FAIL("{}", status.message());
//                    });
//    if constexpr (kind == TypeKind::TIMESTAMP) {
//      if (asLocalTime) {
//        copy.toGMT(Timestamp::defaultTimezone());
//      }
//    }
//    return std::make_shared<ConstantVector<T>>(
//        pool, size, false, type, std::move(copy));
//  }
//}

std::unique_ptr<dwio::common::BufferedInput> createBufferedInput(
    const FileHandle& fileHandle,
    const dwio::common::ReaderOptions& readerOpts,
    const ConnectorQueryCtx* connectorQueryCtx,
    std::shared_ptr<io::IoStatistics> ioStats,
    std::shared_ptr<filesystems::File::IoStats> fsStats,
    folly::Executor* executor) {
  if (connectorQueryCtx->cache()) {
    return std::make_unique<dwio::common::CachedBufferedInput>(
        fileHandle.file,
        dwio::common::MetricsLog::voidLog(),
        fileHandle.uuid.id(),
        connectorQueryCtx->cache(),
        Connector::getTracker(
            connectorQueryCtx->scanId(), readerOpts.loadQuantum()),
        fileHandle.groupId.id(),
        ioStats,
        std::move(fsStats),
        executor,
        readerOpts);
  }
  return std::make_unique<dwio::common::DirectBufferedInput>(
      fileHandle.file,
      dwio::common::MetricsLog::voidLog(),
      fileHandle.uuid.id(),
      Connector::getTracker(
          connectorQueryCtx->scanId(), readerOpts.loadQuantum()),
      fileHandle.groupId.id(),
      std::move(ioStats),
      std::move(fsStats),
      executor,
      readerOpts);
}

velox::core::TypedExprPtr extractFiltersFromRemainingFilter(
    const core::TypedExprPtr& expr,
    core::ExpressionEvaluator* evaluator,
    bool negated,
    SubfieldFilters& filters,
    double& sampleRate) {
  auto* call = dynamic_cast<const core::CallTypedExpr*>(expr.get());
  if (call == nullptr) {
    return expr;
  }
  Filter* oldFilter = nullptr;
  try {
    Subfield subfield;
    if (auto filter = exec::ExprToSubfieldFilterParser::getInstance()
                          ->leafCallToSubfieldFilter(
                              *call, subfield, evaluator, negated)) {
      if (auto it = filters.find(subfield); it != filters.end()) {
        oldFilter = it->second.get();
        filter = filter->mergeWith(oldFilter);
      }
      filters.insert_or_assign(std::move(subfield), std::move(filter));
      return nullptr;
    }
  } catch (const VeloxException&) {
    LOG(WARNING) << "Unexpected failure when extracting filter for: "
                 << expr->toString();
    if (oldFilter) {
      LOG(WARNING) << "Merging with " << oldFilter->toString();
    }
  }

  if (isNotExpr(expr, call, evaluator)) {
    auto inner = extractFiltersFromRemainingFilter(
        call->inputs()[0], evaluator, !negated, filters, sampleRate);
    return inner ? replaceInputs(call, {inner}) : nullptr;
  }

  if ((call->name() == "and" && !negated) ||
      (call->name() == "or" && negated)) {
    auto lhs = extractFiltersFromRemainingFilter(
        call->inputs()[0], evaluator, negated, filters, sampleRate);
    auto rhs = extractFiltersFromRemainingFilter(
        call->inputs()[1], evaluator, negated, filters, sampleRate);
    if (!lhs) {
      return rhs;
    }
    if (!rhs) {
      return lhs;
    }
    return replaceInputs(call, {lhs, rhs});
  }
  if (!negated) {
    double rate = getPrestoSampleRate(expr, call, evaluator);
    if (rate != -1) {
      sampleRate *= rate;
      return nullptr;
    }
  }
  return expr;
}

} // namespace facebook::velox::connector::hive_common
