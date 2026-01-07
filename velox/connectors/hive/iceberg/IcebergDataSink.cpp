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

#include "velox/connectors/hive/iceberg/IcebergDataSink.h"

#include <folly/json.h>
#include "velox/common/base/Fs.h"
#include "velox/common/encode/Base64.h"
#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/connectors/hive/HiveConnectorUtil.h"
#include "velox/connectors/hive/PartitionIdGenerator.h"
#include "velox/connectors/hive/iceberg/DataFileStatsCollector.h"
#include "velox/connectors/hive/iceberg/IcebergColumnHandle.h"
#include "velox/connectors/hive/iceberg/IcebergPartitionIdGenerator.h"
#include "velox/connectors/hive/iceberg/IcebergStatsCollector.h"
#include "velox/connectors/hive/iceberg/TransformFactory.h"
#include "velox/connectors/hive/iceberg/WriterOptionsAdapter.h"
#include "velox/dwio/common/SortingWriter.h"

#ifdef VELOX_ENABLE_PARQUET
#include "velox/connectors/hive/iceberg/IcebergParquetStatsCollector.h"
#include "velox/dwio/parquet/writer/Writer.h"
#endif
#include "velox/dwio/common/TypeWithId.h"
#include "velox/dwio/dwrf/common/Config.h"
#include "velox/dwio/dwrf/common/Statistics.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/SortBuffer.h"
#include "velox/type/Type.h"

namespace facebook::velox::connector::hive::iceberg {

namespace {

constexpr std::string_view kNotClusteredRowsErrorMsg =
    "Incoming records violate the writer assumption that records are clustered by spec and \n by partition within each spec. Either cluster the incoming records or switch to fanout writers.\nEncountered records that belong to already closed files:\n";

#define WRITER_NON_RECLAIMABLE_SECTION_GUARD(index)       \
  memory::NonReclaimableSectionGuard nonReclaimableGuard( \
      writerInfo_[(index)]->nonReclaimableSectionHolder.get())

std::string toJson(const std::vector<folly::dynamic>& partitionValues) {
  folly::dynamic jsonObject = folly::dynamic::object();
  folly::dynamic valuesArray = folly::dynamic::array();
  for (const auto& value : partitionValues) {
    valuesArray.push_back(value);
  }
  jsonObject["partitionValues"] = valuesArray;
  return folly::toJson(jsonObject);
}

template <TypeKind Kind>
folly::dynamic extractPartitionValue(
    const DecodedVector* block,
    vector_size_t row) {
  using T = typename TypeTraits<Kind>::NativeType;
  return block->valueAt<T>(row);
}

template <>
folly::dynamic extractPartitionValue<TypeKind::VARCHAR>(
    const DecodedVector* block,
    vector_size_t row) {
  auto value = block->valueAt<StringView>(row);
  return value.str();
}

template <>
folly::dynamic extractPartitionValue<TypeKind::VARBINARY>(
    const DecodedVector* block,
    vector_size_t row) {
  auto value = block->valueAt<StringView>(row);
  return value.str();
}

template <>
folly::dynamic extractPartitionValue<TypeKind::TIMESTAMP>(
    const DecodedVector* block,
    vector_size_t row) {
  auto timestamp = block->valueAt<Timestamp>(row);
  return timestamp.toMicros();
}

} // namespace

std::pair<std::string, std::string> IcebergFileNameGenerator::gen(
    std::optional<uint32_t> bucketId,
    const std::shared_ptr<const HiveInsertTableHandle> insertTableHandle,
    const ConnectorQueryCtx& connectorQueryCtx,
    bool commitRequired) const {
  auto targetFileName = insertTableHandle->locationHandle()->targetFileName();
  if (targetFileName.empty()) {
    targetFileName = fmt::format("{}", makeUuid());
  }
  auto fileFormat =
      dwio::common::FileFormatName::toName(insertTableHandle->storageFormat());
  auto fileName = fmt::format("{}.{}", targetFileName, fileFormat);
  return {fileName, fileName};
}

folly::dynamic IcebergFileNameGenerator::serialize() const {
  VELOX_UNREACHABLE("Unexpected code path, implement serialize() first.");
}

std::string IcebergFileNameGenerator::toString() const {
  return "IcebergFileNameGenerator";
}

IcebergInsertTableHandle::IcebergInsertTableHandle(
    std::vector<IcebergColumnHandlePtr> inputColumns,
    LocationHandlePtr locationHandle,
    std::shared_ptr<const IcebergPartitionSpec> partitionSpec,
    memory::MemoryPool* pool,
    dwio::common::FileFormat tableStorageFormat,
    const std::vector<IcebergSortingColumn>& sortedBy,
    std::optional<common::CompressionKind> compressionKind,
    const std::unordered_map<std::string, std::string>& serdeParameters,
    std::shared_ptr<const FileNameGenerator> fileNameGenerator,
    WriteKind writeKind)
    : HiveInsertTableHandle(
          std::vector<std::shared_ptr<const HiveColumnHandle>>(
              inputColumns.begin(),
              inputColumns.end()),
          std::move(locationHandle),
          tableStorageFormat,
          nullptr,
          compressionKind,
          serdeParameters,
          nullptr,
          false,
          std::move(fileNameGenerator)),
      partitionSpec_(std::move(partitionSpec)),
      columnTransforms_(
          partitionSpec_
              ? parsePartitionTransformSpecs(partitionSpec_->fields, pool)
              : std::vector<std::shared_ptr<Transform>>{}),
      sortedBy_(sortedBy) {
  VELOX_USER_CHECK(
      !inputColumns_.empty(),
      "Input columns cannot be empty for Iceberg tables.");
  VELOX_USER_CHECK_NOT_NULL(
      locationHandle_, "Location handle is required for Iceberg tables.");
  VELOX_USER_CHECK(
      isSupportedFileFormat(tableStorageFormat),
      "Unsupported file format for writing Iceberg tables: {}",
      dwio::common::FileFormatName::toName(tableStorageFormat));
}

namespace {

// Creates partition channels by mapping partition spec fields to input column
// indices. For each field in the partition spec, finds the corresponding
// partition key column in the input columns and records its index.
//
// @param inputColumns The input columns from the insert table handle.
// @param partitionSpec The Iceberg partition specification, or nullptr if
// unpartitioned.
// @return A vector of column indices representing the partition channels. Each
// index corresponds to a partition field in the spec and points to the
// matching partition key column in the input. Returns an empty vector if
// partitionSpec is nullptr.
std::vector<column_index_t> createPartitionChannels(
    const std::vector<HiveColumnHandlePtr>& inputColumns,
    const std::shared_ptr<const IcebergPartitionSpec>& partitionSpec) {
  std::vector<column_index_t> channels;
  if (!partitionSpec) {
    return channels;
  }

  // Build a map from partition key column names to their indices in the input.
  std::unordered_map<std::string, column_index_t> partitionKeyMap;
  for (auto i = 0; i < inputColumns.size(); ++i) {
    if (inputColumns[i]->isPartitionKey()) {
      partitionKeyMap[inputColumns[i]->name()] = i;
    }
  }

  // For each field in the partition spec, find its corresponding input column
  // index.
  channels.reserve(partitionSpec->fields.size());
  for (const auto& field : partitionSpec->fields) {
    if (auto it = partitionKeyMap.find(field.name);
        it != partitionKeyMap.end()) {
      channels.push_back(it->second);
    }
  }

  return channels;
}

std::vector<column_index_t> createDataChannels(
    const IcebergInsertTableHandlePtr& insertTableHandle) {
  std::vector<column_index_t> dataChannels(
      insertTableHandle->inputColumns().size());
  std::iota(dataChannels.begin(), dataChannels.end(), 0);
  return dataChannels;
}

std::shared_ptr<dwio::common::DataFileStatistics> collectWriterStats(
    const IcebergInsertTableHandlePtr& insertTableHandle,
    const std::vector<std::unique_ptr<dwio::common::Writer>>& writers,
    const std::vector<std::shared_ptr<WriterInfo>>& writerInfo,
    std::vector<int64_t>& reportedRowsPerWriter,
    size_t index) {
  auto stats = writers[index]->dataFileStats();

#ifdef VELOX_ENABLE_PARQUET
  if (insertTableHandle->storageFormat() == dwio::common::FileFormat::PARQUET) {
    return stats;
  }
#endif

  if (reportedRowsPerWriter.size() <= index) {
    reportedRowsPerWriter.resize(index + 1, 0);
  }
  const int64_t totalRows = writerInfo[index]->numWrittenRows;
  const int64_t thisFileRows = totalRows - reportedRowsPerWriter[index];
  reportedRowsPerWriter[index] = totalRows;

  if (stats == nullptr) {
    stats = std::make_shared<dwio::common::DataFileStatistics>();
  }
  stats->numRecords = thisFileRows;
  return stats;
}

// Creates a RowType schema for transformed partition values based on the
// partition specification. This RowType is used to wrap the transformed
// partition columns before passing them to the partition ID generator.
//
// For each partition field in the spec:
// - The column type is the result type of the partition transform (e.g.,
//   INTEGER for year transform, DATE for day transform).
// - The column name is the source column name for identity transforms, or
//   "columnName_transformName" for non-identity transforms (e.g., "birth_year"
//   for a year transform on a birth column).
//
// @param partitionSpec The Iceberg partition specification, or nullptr if
// unpartitioned.
// @return A RowType containing one column per partition field with appropriate
// names and types. Returns nullptr if partitionSpec is nullptr.
RowTypePtr createPartitionRowType(
    const std::shared_ptr<const IcebergPartitionSpec>& partitionSpec) {
  if (!partitionSpec) {
    return nullptr;
  }

  std::vector<TypePtr> partitionKeyTypes;
  std::vector<std::string> partitionKeyNames;

  // Build column names and types for each partition field.
  // Identity transforms use the source column name directly.
  // Non-identity transforms use "columnName_transformName" format.
  for (const auto& field : partitionSpec->fields) {
    partitionKeyTypes.emplace_back(field.type);
    std::string key = field.transformType == TransformType::kIdentity
        ? field.name
        : fmt::format(
              "{}_{}",
              field.name,
              TransformTypeName::toName(field.transformType));
    partitionKeyNames.emplace_back(std::move(key));
  }

  return ROW(std::move(partitionKeyNames), std::move(partitionKeyTypes));
}

// Builds the CommitTaskData JSON object Presto consumes for one written file.
// Iceberg "content" is derived from the sink's WriteKind: INSERT (kData) emits
// "DATA"; the V3 deletion-vector path (kDeletionVector) emits
// "POSITION_DELETES" because Iceberg classifies deletion vectors as a
// Puffin-encoded form of position deletes for catalog accounting.
folly::dynamic buildIcebergCommitData(
    std::string path,
    uint64_t fileSizeInBytes,
    folly::dynamic metrics,
    int32_t partitionSpecId,
    std::string fileFormat,
    bool isDeletionVector) {
  folly::dynamic commitData = folly::dynamic::object;
  commitData["path"] = std::move(path);
  commitData["fileSizeInBytes"] = fileSizeInBytes;
  commitData["metrics"] = std::move(metrics);
  commitData["partitionSpecJson"] = partitionSpecId;
  // Sort order evolution is not supported. Default id 0 (unsorted order).
  commitData["sortOrderId"] = 0;
  commitData["fileFormat"] = std::move(fileFormat);
  commitData["content"] = isDeletionVector ? "POSITION_DELETES" : "DATA";
  return commitData;
}

// Recursively records the "iceberg.id" attribute for 'node' and its
// descendants, keyed by pre-order schema node id (matching the DWRF writer's
// footer node ids). 'fieldId' is the field-id tree aligned to 'node'.
void stampFieldIdAttributes(
    const dwio::common::TypeWithId& node,
    const dwio::common::ParquetFieldId& fieldId,
    std::unordered_map<
        uint32_t,
        std::vector<std::pair<std::string, std::string>>>& attributes) {
  attributes[node.id()] = {{"iceberg.id", std::to_string(fieldId.fieldId)}};
  const auto numChildren =
      std::min<size_t>(node.size(), fieldId.children.size());
  for (size_t i = 0; i < numChildren; ++i) {
    stampFieldIdAttributes(
        *node.childAt(static_cast<uint32_t>(i)),
        fieldId.children.at(i),
        attributes);
  }
}

// Builds the per-node "iceberg.id" attribute map for the DWRF writer from the
// Iceberg input column handles, aligned to 'schema' (the written row type).
std::unordered_map<uint32_t, std::vector<std::pair<std::string, std::string>>>
buildDwrfSchemaAttributes(
    const RowTypePtr& schema,
    const std::vector<std::shared_ptr<const HiveColumnHandle>>& inputColumns) {
  std::unordered_map<uint32_t, std::vector<std::pair<std::string, std::string>>>
      attributes;
  if (schema == nullptr) {
    return attributes;
  }
  const auto schemaWithId = dwio::common::TypeWithId::create(schema);
  const auto numColumns =
      std::min<size_t>(schemaWithId->size(), inputColumns.size());
  for (size_t i = 0; i < numColumns; ++i) {
    const auto icebergHandle =
        std::dynamic_pointer_cast<const IcebergColumnHandle>(inputColumns[i]);
    if (icebergHandle != nullptr) {
      stampFieldIdAttributes(
          *schemaWithId->childAt(static_cast<uint32_t>(i)),
          icebergHandle->nestedField().toParquetFieldId(),
          attributes);
    }
  }
  return attributes;
}

} // namespace

IcebergDataSink::IcebergDataSink(
    RowTypePtr inputType,
    IcebergInsertTableHandlePtr insertTableHandle,
    const ConnectorQueryCtx* connectorQueryCtx,
    CommitStrategy commitStrategy,
    const std::shared_ptr<const HiveConfig>& hiveConfig)
    : IcebergDataSink(
          std::move(inputType),
          insertTableHandle,
          connectorQueryCtx,
          commitStrategy,
          hiveConfig,
          [&insertTableHandle]() {
            const auto& inputColumns = insertTableHandle->inputColumns();
            const auto& partitionSpec = insertTableHandle->partitionSpec();
            std::unordered_map<std::string, column_index_t> partitionKeyMap;
            for (auto i = 0; i < inputColumns.size(); ++i) {
              if (inputColumns[i]->isPartitionKey()) {
                partitionKeyMap[inputColumns[i]->name()] = i;
              }
            }
            std::vector<column_index_t> channels;
            channels.reserve(partitionSpec->fields.size());
            for (const auto& field : partitionSpec->fields) {
              if (auto it = partitionKeyMap.find(field.name);
                  it != partitionKeyMap.end()) {
                channels.push_back(it->second);
              }
            }
            return channels;
          }(),
          [&insertTableHandle]() {
            std::vector<column_index_t> channels(
                insertTableHandle->inputColumns().size());
            std::iota(channels.begin(), channels.end(), 0);
            return channels;
          }()) {}

IcebergDataSink::IcebergDataSink(
    RowTypePtr inputType,
    IcebergInsertTableHandlePtr insertTableHandle,
    const ConnectorQueryCtx* connectorQueryCtx,
    CommitStrategy commitStrategy,
    const std::shared_ptr<const HiveConfig>& hiveConfig,
    const std::vector<column_index_t>& partitionChannels,
    const std::vector<column_index_t>& dataChannels)
    : HiveDataSink(
          inputType,
          insertTableHandle,
          connectorQueryCtx,
          commitStrategy,
          hiveConfig,
          0,
          nullptr,
          partitionChannels,
          dataChannels,
          !partitionChannels.empty()
              ? std::make_unique<IcebergPartitionIdGenerator>(
                    partitionChannels,
                    hiveConfig->maxPartitionsPerWriters(
                        connectorQueryCtx->sessionProperties()),
                    connectorQueryCtx->memoryPool(),
                    insertTableHandle->columnTransforms(),
                    hiveConfig->isPartitionPathAsLowerCase(
                        connectorQueryCtx->sessionProperties()))
              : nullptr),
      icebergInsertTableHandle_(insertTableHandle),
      fanoutEnabled_(
          hiveConfig_->fanoutEnabled(connectorQueryCtx_->sessionProperties())),
      currentWriterId_(0) {
  if (isPartitioned()) {
    partitionData_.resize(maxOpenWriters_);
  }
  const auto& inputColumns = insertTableHandle_->inputColumns();

  std::function<IcebergDataFileStatsSettings(
      const IcebergNestedField&, const TypePtr&, bool)>
      buildNestedField = [&](const IcebergNestedField& f,
                             const TypePtr& type,
                             bool skipBounds) -> IcebergDataFileStatsSettings {
    VELOX_CHECK_NOT_NULL(type, "Input column type cannot be null.");
    bool currentSkipBounds = skipBounds || type->isMap() || type->isArray();
    IcebergDataFileStatsSettings field(f.id, currentSkipBounds);
    if (!f.children.empty()) {
      VELOX_CHECK_EQ(f.children.size(), type->size());
      field.children.reserve(f.children.size());
      if (type->isRow()) {
        auto rowType = asRowType(type);
        for (size_t i = 0; i < f.children.size(); ++i) {
          field.children.push_back(
              std::make_unique<IcebergDataFileStatsSettings>(buildNestedField(
                  f.children[i], rowType->childAt(i), currentSkipBounds)));
        }
      } else if (type->isArray()) {
        auto arrayType = type->asArray();
        field.children.push_back(
            std::make_unique<IcebergDataFileStatsSettings>(buildNestedField(
                f.children[0], arrayType.elementType(), currentSkipBounds)));
      } else if (type->isMap()) {
        auto mapType = type->asMap();
        for (size_t i = 0; i < f.children.size(); ++i) {
          field.children.push_back(
              std::make_unique<IcebergDataFileStatsSettings>(buildNestedField(
                  f.children[i], mapType.childAt(i), currentSkipBounds)));
        }
      }
    }
    return field;
  };

  statsSettings_ = std::make_shared<
      std::vector<std::unique_ptr<dwio::common::DataFileStatsSettings>>>();
  for (const auto& columnHandle : inputColumns) {
    auto icebergColumnHandle =
        std::dynamic_pointer_cast<const IcebergColumnHandle>(columnHandle);
    VELOX_CHECK_NOT_NULL(icebergColumnHandle, "Invalid IcebergColumnHandle.");
    statsSettings_->push_back(
        std::make_unique<IcebergDataFileStatsSettings>(buildNestedField(
            icebergColumnHandle->nestedField(),
            icebergColumnHandle->dataType(),
            false)));
  }

  icebergStatsCollector_ =
      std::make_unique<DataFileStatsCollector>(statsSettings_);

  const auto& sortedBy = insertTableHandle->sortedBy();
  if (!sortedBy.empty()) {
    sortColumnIndices_.reserve(sortedBy.size());
    sortCompareFlags_.reserve(sortedBy.size());
    for (auto i = 0; i < sortedBy.size(); ++i) {
      auto columnIndex =
          inputType_->getChildIdxIfExists(sortedBy[i].sortColumn());
      if (columnIndex.has_value()) {
        sortColumnIndices_.push_back(columnIndex.value());
        sortCompareFlags_.push_back(
            {sortedBy[i].sortOrder().isNullsFirst(),
             sortedBy[i].sortOrder().isAscending(),
             false,
             CompareFlags::NullHandlingMode::kNullAsValue});
      }
    }
    sortWrite_ = !sortColumnIndices_.empty();
  }

  // Build the column handle list once for whichever format-specific stats
  // collector applies.
  std::vector<IcebergColumnHandlePtr> columnHandles;
  columnHandles.reserve(insertTableHandle->inputColumns().size());
  for (auto& column : insertTableHandle->inputColumns()) {
    columnHandles.emplace_back(
        checkedPointerCast<const IcebergColumnHandle>(column));
  }

  // Statistics extraction and field-id wiring are format-specific; the factory
  // returns the matching collector, or nullptr for formats without support.
  statsCollector_ = IcebergStatsCollector::create(
      insertTableHandle->storageFormat(), columnHandles, inputType_);
}

std::vector<std::string> IcebergDataSink::commitMessage() const {
  std::vector<std::string> commitTasks;
  commitTasks.reserve(writerInfo_.size());

  for (auto i = 0; i < writerInfo_.size(); ++i) {
    const auto& info = writerInfo_.at(i);
    VELOX_CHECK_NOT_NULL(info);

    // Following metadata (json format) is consumed by Presto CommitTaskData.
    // It contains the minimal subset of metadata.
    folly::dynamic commitData =
        folly::dynamic::object(
            "path",
            (fs::path(info->writerParameters.writeDirectory()) /
             info->writerParameters.writeFileName())
                .string())(
            "fileSizeInBytes", ioStats_.at(i)->rawBytesWritten())(
            // Sort order evolution is not supported. Default id 0.
            "sortOrderId", 0)(
            "partitionSpecJson",
            icebergInsertTableHandle_->partitionSpec()
                ? icebergInsertTableHandle_->partitionSpec()->specId
                : 0)(
            "fileFormat",
            toManifestFormatString(icebergInsertTableHandle_->storageFormat()))(
            "content", "DATA");
    if (dataFileStats_[i] != nullptr) {
      commitData["metrics"] = dataFileStats_[i]->toJson();
      commitData["splitOffsets"] = dataFileStats_[i]->splitOffsetsAsJson();
    }
    if (!(partitionData_.empty() || partitionData_[i].empty())) {
      commitData["partitionDataJson"] = toJson(partitionData_[i]);
    }
    auto commitDataJson = folly::toJson(commitData);
    commitTasks.push_back(commitDataJson);
  }
  return commitTasks;
}

void IcebergDataSink::splitInputRowsAndEnsureWriters() {
  std::fill(partitionSizes_.begin(), partitionSizes_.end(), 0);

  const auto numRows = partitionIds_.size();
  for (auto row = 0; row < numRows; ++row) {
    auto id = getIcebergWriterId(row);
    uint32_t index = ensureWriter(id);

    updatePartitionRows(index, numRows, row);

    if (!partitionData_[index].empty()) {
      continue;
    }
    buildPartitionData(index);
  }

  for (auto i = 0; i < partitionSizes_.size(); ++i) {
    if (partitionSizes_[i] != 0) {
      VELOX_CHECK_NOT_NULL(partitionRows_[i]);
      partitionRows_[i]->setSize(partitionSizes_[i] * sizeof(vector_size_t));
    }
  }
}

void IcebergDataSink::computePartition(const RowVectorPtr& input) {
  VELOX_CHECK(isPartitioned());
  partitionIdGenerator_->run(input, partitionIds_);
}

void IcebergDataSink::appendData(RowVectorPtr input) {
  checkRunning();
  if (!isPartitioned()) {
    const auto index = ensureWriter(WriterId::unpartitionedId());
    write(index, input);
    return;
  }

  computePartition(input);

  if (fanoutEnabled_) {
    splitInputRowsAndEnsureWriters();

    for (auto index = 0; index < writers_.size(); ++index) {
      const vector_size_t partitionSize = partitionSizes_[index];
      if (partitionSize == 0) {
        continue;
      }

      const RowVectorPtr writerInput = partitionSize == input->size()
          ? input
          : exec::wrap(partitionSize, partitionRows_[index], input);
      write(index, writerInput);
    }
  } else { // Clustered mode.
    std::fill(partitionSizes_.begin(), partitionSizes_.end(), 0);
    const auto numRows = input->size();
    uint32_t index = 0;
    for (auto row = 0; row < numRows; ++row) {
      auto id = getIcebergWriterId(row);
      index = ensureWriter(id);
      if (currentWriterId_ != index) {
        clusteredWrite(input, currentWriterId_);
        closeWriter(currentWriterId_);
        completedWriterIds_.insert(currentWriterId_);
        VELOX_USER_CHECK_EQ(
            completedWriterIds_.count(index),
            0,
            "{}",
            kNotClusteredRowsErrorMsg);
        currentWriterId_ = index;
      }
      updatePartitionRows(index, numRows, row);
      buildPartitionData(index);
    }
    clusteredWrite(input, index);
  }
}

void IcebergDataSink::buildPartitionData(int32_t index) {
  std::vector<folly::dynamic> partitionValues(partitionChannels_.size());
  auto icebergPartitionIdGenerator =
      dynamic_cast<const IcebergPartitionIdGenerator*>(
          partitionIdGenerator_.get());
  VELOX_CHECK_NOT_NULL(icebergPartitionIdGenerator);
  const RowVectorPtr transformedValues =
      icebergPartitionIdGenerator->partitionValues();
  for (auto i = 0; i < partitionChannels_.size(); ++i) {
    auto block = transformedValues->childAt(i);
    if (block->isNullAt(index)) {
      partitionValues[i] = nullptr;
    } else {
      DecodedVector decoded(*block);
      partitionValues[i] = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          extractPartitionValue, block->typeKind(), &decoded, index);
    }
  }
  partitionData_[index] = partitionValues;
}

void IcebergDataSink::clusteredWrite(RowVectorPtr input, int32_t writerIdx) {
  if (partitionSizes_[writerIdx] != 0) {
    VELOX_CHECK_NOT_NULL(partitionRows_[writerIdx]);
    partitionRows_[writerIdx]->setSize(
        partitionSizes_[writerIdx] * sizeof(vector_size_t));
  }
  const vector_size_t partitionSize = partitionSizes_[writerIdx];
  const RowVectorPtr writerInput = partitionSize == input->size()
      ? input
      : exec::wrap(partitionSize, partitionRows_[writerIdx], input);
  write(writerIdx, writerInput);
}

WriterId IcebergDataSink::getIcebergWriterId(size_t row) const {
  std::optional<int32_t> partitionId;
  if (isPartitioned()) {
    VELOX_CHECK_LT(partitionIds_[row], std::numeric_limits<uint32_t>::max());
    partitionId = static_cast<uint32_t>(partitionIds_[row]);
  }

  std::optional<int32_t> bucketId;
  if (isBucketed()) {
    bucketId = bucketIds_[row];
  }
  return WriterId{partitionId, std::nullopt};
}

std::shared_ptr<dwio::common::WriterOptions>
IcebergDataSink::createWriterOptions(size_t writerIndex) const {
  auto options = HiveDataSink::createWriterOptions(writerIndex);
  options->fileStatsCollector = icebergStatsCollector_.get();

  // Wire Iceberg field ids into the writer options. The collector applies the
  // wiring only when 'options' matches its format and is a no-op otherwise.
  if (statsCollector_ != nullptr) {
    statsCollector_->configureWriterOptions(*options);
  }

#ifdef VELOX_ENABLE_PARQUET
  if (icebergInsertTableHandle_->storageFormat() ==
      dwio::common::FileFormat::PARQUET) {
    auto parquetOptions =
        std::dynamic_pointer_cast<parquet::WriterOptions>(options);
    VELOX_CHECK_NOT_NULL(parquetOptions);

    std::function<parquet::ParquetFieldId(const IcebergDataFileStatsSettings&)>
        convertField =
            [&convertField](const IcebergDataFileStatsSettings& icebergField)
        -> parquet::ParquetFieldId {
      parquet::ParquetFieldId parquetField;
      parquetField.fieldId = icebergField.fieldId;
      for (const auto& child : icebergField.children) {
        parquetField.children.push_back(convertField(*child));
      }
      return parquetField;
    };

    std::vector<parquet::ParquetFieldId> parquetFieldIds;
    for (const auto& setting : *statsSettings_) {
      const auto* icebergSetting =
          static_cast<const IcebergDataFileStatsSettings*>(setting.get());
      parquetFieldIds.push_back(convertField(*icebergSetting));
    }

    parquetOptions->parquetFieldIds = parquetFieldIds;
    parquetOptions->parquetWriteTimestampTimeZone = std::nullopt;
    parquetOptions->parquetWriteTimestampUnit =
        TimestampPrecision::kMicroseconds;
  }
#endif

  // For DWRF/ORC, Iceberg field ids are carried as per-type "iceberg.id"
  // attributes stamped into the footer. Build the per-node attribute map from
  // the input column handles so the reader can resolve columns by field id.
  if (auto dwrfOptions =
          std::dynamic_pointer_cast<dwrf::WriterOptions>(options)) {
    dwrfOptions->schemaAttributes = buildDwrfSchemaAttributes(
        std::dynamic_pointer_cast<const RowType>(dwrfOptions->schema),
        icebergInsertTableHandle_->inputColumns());
  }

  return options;
}


std::string IcebergDataSink::getPartitionName(uint32_t partitionId) const {
  return partitionIdGenerator_->partitionName(partitionId);
}

std::shared_ptr<dwio::common::DataFileStatistics>
IcebergDataSink::collectWriterStats(size_t index) {
  return facebook::velox::connector::hive::iceberg::collectWriterStats(
      icebergInsertTableHandle_,
      writers_,
      writerInfo_,
      reportedRowsPerWriter_,
      index);
}

void IcebergDataSink::rotateWriter(size_t index) {
  VELOX_CHECK_LT(index, writers_.size());
  VELOX_CHECK_NOT_NULL(writers_[index]);

  dataFileStats_.push_back(collectWriterStats(index));
  HiveDataSink::rotateWriter(index);
}

void IcebergDataSink::closeInternal() {
  VELOX_CHECK_NE(state_, State::kRunning);
  VELOX_CHECK_NE(state_, State::kFinishing);

  common::testutil::TestValue::adjust(
      "facebook::velox::connector::hive::IcebergDataSink::closeInternal", this);

  if (state_ == State::kClosed) {
    for (int i = 0; i < writers_.size(); ++i) {
      if (writers_[i]) {
        WRITER_NON_RECLAIMABLE_SECTION_GUARD(i);
        writers_[i]->close();
        dataFileStats_.push_back(collectWriterStats(i));
      }
    }
  } else {
    for (int i = 0; i < writers_.size(); ++i) {
      if (writers_[i]) {
        WRITER_NON_RECLAIMABLE_SECTION_GUARD(i);
        writers_[i]->abort();
      }
    }
  }
}

void IcebergDataSink::closeWriter(int32_t index) {
  common::testutil::TestValue::adjust(
      "facebook::velox::connector::hive::iceberg::IcebergDataSink::closeWriter",
      this);

  if (writers_[index]) {
    WRITER_NON_RECLAIMABLE_SECTION_GUARD(index);
    if (sortWrite()) {
      finishWriter(index);
    }
    writers_[index]->close();
    dataFileStats_.push_back(collectWriterStats(index));
    writers_[index] = nullptr;
  }
}

bool IcebergDataSink::finishWriter(int32_t index) {
  if (!sortWrite()) {
    return true;
  }

  if (writers_[index]) {
    const uint64_t startTimeMs = getCurrentTimeMs();
    if (!writers_[index]->finish()) {
      return false;
    }
    if (getCurrentTimeMs() - startTimeMs > sortWriterFinishTimeSliceLimitMs_) {
      return false;
    }
  }
  return true;
}

bool IcebergDataSink::finish() {
  // Flush is reentry state.
  setState(State::kFinishing);

  // As for now, only sorted writer needs flush buffered data. For non-sorted
  // writer, data is directly written to the underlying file writer.
  if (!sortWrite()) {
    return true;
  }

  // TODO: we might refactor to move the data sorting logic into hive data sink.
  const uint64_t startTimeMs = getCurrentTimeMs();
  for (auto i = 0; i < writers_.size(); ++i) {
    WRITER_NON_RECLAIMABLE_SECTION_GUARD(i);
    if (writers_[i] && !writers_[i]->finish()) {
      return false;
    }
    if (getCurrentTimeMs() - startTimeMs > sortWriterFinishTimeSliceLimitMs_) {
      return false;
    }
  }
  return true;
}

std::vector<std::string> IcebergDataSink::close() {
  if (state_ == State::kRunning) {
    finish();
  }
  return HiveDataSink::close();
}

std::unique_ptr<facebook::velox::dwio::common::Writer>
IcebergDataSink::maybeCreateBucketSortWriter(
    std::unique_ptr<facebook::velox::dwio::common::Writer> writer) {
  if (!sortWrite()) {
    return writer;
  }
  auto sortPool = writerInfo_.back()->sortPool.get();
  VELOX_CHECK_NOT_NULL(sortPool);
  auto sortBuffer = std::make_unique<exec::SortBuffer>(
      inputType_,
      sortColumnIndices_,
      sortCompareFlags_,
      sortPool,
      writerInfo_.back()->nonReclaimableSectionHolder.get(),
      connectorQueryCtx_->prefixSortConfig(),
      spillConfig_,
      writerInfo_.back()->spillStats.get());
  return std::make_unique<dwio::common::SortingWriter>(
      std::move(writer),
      std::move(sortBuffer),
      hiveConfig_->sortWriterMaxOutputRows(
          connectorQueryCtx_->sessionProperties()),
      hiveConfig_->sortWriterMaxOutputBytes(
          connectorQueryCtx_->sessionProperties()),
      sortWriterFinishTimeSliceLimitMs_);
}

IcebergSortingColumn::IcebergSortingColumn(
    const std::string& sortColumn,
    const core::SortOrder& sortOrder)
    : sortColumn_(sortColumn), sortOrder_(sortOrder) {
  VELOX_USER_CHECK(!sortColumn_.empty(), "iceberg sort column must be set.");
}

const std::string& IcebergSortingColumn::sortColumn() const {
  return sortColumn_;
}

const core::SortOrder& IcebergSortingColumn::sortOrder() const {
  return sortOrder_;
}

folly::dynamic IcebergSortingColumn::serialize() const {
  VELOX_UNREACHABLE("Unexpected code path, implement serialize() first.");
}


} // namespace facebook::velox::connector::hive::iceberg
