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

#include "TableHandleBase.h"
#include "velox/common/compression/Compression.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/lakehouse/iceberg/IcebergConfig.h"
#include "velox/connectors/lakehouse/iceberg/PartitionIdGenerator.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/Writer.h"
#include "velox/dwio/common/WriterFactory.h"
#include "velox/exec/MemoryReclaimer.h"

namespace facebook::velox::connector::lakehouse::iceberg {

class LocationHandle;
using LocationHandlePtr = std::shared_ptr<const LocationHandle>;

/// Location related properties of the Hive table to be written.
class LocationHandle : public ISerializable {
 public:
  enum class TableType {
    /// Write to a new table to be created.
    kNew,
    /// Write to an existing table.
    kExisting,
  };

  LocationHandle(
      std::string targetPath,
      std::string writePath,
      TableType tableType,
      std::string targetFileName = "")
      : targetPath_(std::move(targetPath)),
        targetFileName_(std::move(targetFileName)),
        writePath_(std::move(writePath)),
        tableType_(tableType) {}

  const std::string& targetPath() const {
    return targetPath_;
  }

  const std::string& targetFileName() const {
    return targetFileName_;
  }

  const std::string& writePath() const {
    return writePath_;
  }

  TableType tableType() const {
    return tableType_;
  }

  std::string toString() const;

  static void registerSerDe();

  folly::dynamic serialize() const override;

  static LocationHandlePtr create(const folly::dynamic& obj);

  static const std::string tableTypeName(LocationHandle::TableType type);

  static LocationHandle::TableType tableTypeFromName(const std::string& name);

 private:
  // Target directory path.
  const std::string targetPath_;
  // If non-empty, use this name instead of generating our own.
  const std::string targetFileName_;
  // Staging directory path.
  const std::string writePath_;
  // Whether the table to be written is new, already existing or temporary.
  const TableType tableType_;
};

class IcebergSortingColumn : public ISerializable {
 public:
  IcebergSortingColumn(
      const std::string& sortColumn,
      const core::SortOrder& sortOrder);

  const std::string& sortColumn() const {
    return sortColumn_;
  }

  core::SortOrder sortOrder() const {
    return sortOrder_;
  }

  folly::dynamic serialize() const override;

  static std::shared_ptr<IcebergSortingColumn> deserialize(
      const folly::dynamic& obj,
      void* context);

  std::string toString() const;

  static void registerSerDe();

 private:
  const std::string sortColumn_;
  const core::SortOrder sortOrder_;
};

class IcebergInsertTableHandle;
using IcebergInsertTableHandlePtr = std::shared_ptr<IcebergInsertTableHandle>;

class FileNameGenerator : public ISerializable {
 public:
  virtual ~FileNameGenerator() = default;

  virtual std::pair<std::string, std::string> gen(
      std::optional<uint32_t> bucketId,
      const std::shared_ptr<const IcebergInsertTableHandle> insertTableHandle,
      const ConnectorQueryCtx& connectorQueryCtx,
      bool commitRequired) const = 0;

  virtual std::string toString() const = 0;
};

class IcebergInsertFileNameGenerator : public FileNameGenerator {
 public:
  IcebergInsertFileNameGenerator() {}

  std::pair<std::string, std::string> gen(
      std::optional<uint32_t> bucketId,
      const std::shared_ptr<const IcebergInsertTableHandle> insertTableHandle,
      const ConnectorQueryCtx& connectorQueryCtx,
      bool commitRequired) const override;

  /// Version of file generation that takes icebergConfig into account when
  /// generating file names
  std::pair<std::string, std::string> gen(
      std::optional<uint32_t> bucketId,
      const std::shared_ptr<const IcebergInsertTableHandle> insertTableHandle,
      const ConnectorQueryCtx& connectorQueryCtx,
      const std::shared_ptr<const iceberg::IcebergConfig>& icebergConfig,
      bool commitRequired) const;

  static void registerSerDe();

  folly::dynamic serialize() const override;

  static std::shared_ptr<IcebergInsertFileNameGenerator> deserialize(
      const folly::dynamic& obj,
      void* context);

  std::string toString() const override;
};

/// Represents a request for Hive write.
class IcebergInsertTableHandle : public ConnectorInsertTableHandle {
 public:
  IcebergInsertTableHandle(
      std::vector<ColumnHandlePtr> inputColumns,
      std::shared_ptr<const LocationHandle> locationHandle,
      dwio::common::FileFormat storageFormat = dwio::common::FileFormat::DWRF,
      std::optional<velox::common::CompressionKind> compressionKind = {},
      const std::unordered_map<std::string, std::string>& serdeParameters = {},
      const std::shared_ptr<dwio::common::WriterOptions>& writerOptions =
          nullptr,
      // When this option is set the IcebergDataSink will always write a file even
      // if there's no data. This is useful when the table is bucketed, but the
      // engine handles ensuring a 1 to 1 mapping from task to bucket.
      const bool ensureFiles = false,
      std::shared_ptr<const FileNameGenerator> fileNameGenerator =
          std::make_shared<const IcebergInsertFileNameGenerator>())
      : inputColumns_(std::move(inputColumns)),
        locationHandle_(std::move(locationHandle)),
        storageFormat_(storageFormat),
//        bucketProperty_(std::move(bucketProperty)),
        compressionKind_(compressionKind),
        serdeParameters_(serdeParameters),
        writerOptions_(writerOptions),
        ensureFiles_(ensureFiles),
        fileNameGenerator_(std::move(fileNameGenerator)) {
    if (compressionKind.has_value()) {
      VELOX_CHECK(
          compressionKind.value() != velox::common::CompressionKind_MAX,
          "Unsupported compression type: CompressionKind_MAX");
    }

    if (ensureFiles_) {
      for (const auto& inputColumn : inputColumns_) {
        auto inputColumnBase =
            std::dynamic_pointer_cast<const ColumnHandleBase>(inputColumn);
        VELOX_CHECK(
            inputColumnBase,
            "{}} is not ColumnHandleBase",
            inputColumn->name());
        VELOX_CHECK(
            !inputColumnBase->isPartitionKey(),
            "ensureFiles is not supported with partition keys in the data");
      }
    }
  }

  virtual ~IcebergInsertTableHandle() = default;

  const std::vector<ColumnHandlePtr>& inputColumns() const {
    return inputColumns_;
  }

  const std::shared_ptr<const LocationHandle>& locationHandle() const {
    return locationHandle_;
  }

  std::optional<velox::common::CompressionKind> compressionKind() const {
    return compressionKind_;
  }

  dwio::common::FileFormat storageFormat() const {
    return storageFormat_;
  }

  const std::unordered_map<std::string, std::string>& serdeParameters() const {
    return serdeParameters_;
  }

  const std::shared_ptr<dwio::common::WriterOptions>& writerOptions() const {
    return writerOptions_;
  }

  bool ensureFiles() const {
    return ensureFiles_;
  }

  const std::shared_ptr<const FileNameGenerator>& fileNameGenerator() const {
    return fileNameGenerator_;
  }

  bool supportsMultiThreading() const override {
    return true;
  }

  bool isPartitioned() const;

  bool isBucketed() const;

//  const IcebergBucketProperty* bucketProperty() const;

  bool isExistingTable() const;

  folly::dynamic serialize() const override;

  static IcebergInsertTableHandlePtr create(const folly::dynamic& obj);

  static void registerSerDe();

  std::string toString() const override;

 private:
  const std::vector<ColumnHandlePtr> inputColumns_;
  const std::shared_ptr<const LocationHandle> locationHandle_;
  const dwio::common::FileFormat storageFormat_;
//  const std::shared_ptr<const IcebergBucketProperty> bucketProperty_;
  const std::optional<velox::common::CompressionKind> compressionKind_;
  const std::unordered_map<std::string, std::string> serdeParameters_;
  const std::shared_ptr<dwio::common::WriterOptions> writerOptions_;
  const bool ensureFiles_;
  const std::shared_ptr<const FileNameGenerator> fileNameGenerator_;
};

/// Parameters for Hive writers.
class IcebergWriterParameters {
 public:
  enum class UpdateMode {
    kNew, // Write files to a new directory.
    kOverwrite, // Overwrite an existing directory.
    // Append mode is currently only supported for unpartitioned tables.
    kAppend, // Append to an unpartitioned table.
  };

  /// @param updateMode Write the files to a new directory, or append to an
  /// existing directory or overwrite an existing directory.
  /// @param partitionName Partition name in the typical Hive style, which is
  /// also the partition subdirectory part of the partition path.
  /// @param targetFileName The final name of a file after committing.
  /// @param targetDirectory The final directory that a file should be in after
  /// committing.
  /// @param writeFileName The temporary name of the file that a running writer
  /// writes to. If a running writer writes directory to the target file, set
  /// writeFileName to targetFileName by default.
  /// @param writeDirectory The temporary directory that a running writer writes
  /// to. If a running writer writes directory to the target directory, set
  /// writeDirectory to targetDirectory by default.
  IcebergWriterParameters(
      UpdateMode updateMode,
      std::optional<std::string> partitionName,
      std::string targetFileName,
      std::string targetDirectory,
      std::optional<std::string> writeFileName = std::nullopt,
      std::optional<std::string> writeDirectory = std::nullopt)
      : updateMode_(updateMode),
        partitionName_(std::move(partitionName)),
        targetFileName_(std::move(targetFileName)),
        targetDirectory_(std::move(targetDirectory)),
        writeFileName_(writeFileName.value_or(targetFileName_)),
        writeDirectory_(writeDirectory.value_or(targetDirectory_)) {}

  UpdateMode updateMode() const {
    return updateMode_;
  }

  static std::string updateModeToString(UpdateMode updateMode) {
    switch (updateMode) {
      case UpdateMode::kNew:
        return "NEW";
      case UpdateMode::kOverwrite:
        return "OVERWRITE";
      case UpdateMode::kAppend:
        return "APPEND";
      default:
        VELOX_UNSUPPORTED("Unsupported update mode.");
    }
  }

  const std::optional<std::string>& partitionName() const {
    return partitionName_;
  }

  const std::string& targetFileName() const {
    return targetFileName_;
  }

  const std::string& writeFileName() const {
    return writeFileName_;
  }

  const std::string& targetDirectory() const {
    return targetDirectory_;
  }

  const std::string& writeDirectory() const {
    return writeDirectory_;
  }

 private:
  const UpdateMode updateMode_;
  const std::optional<std::string> partitionName_;
  const std::string targetFileName_;
  const std::string targetDirectory_;
  const std::string writeFileName_;
  const std::string writeDirectory_;
};

struct IcebergWriterInfo {
  IcebergWriterInfo(
      IcebergWriterParameters parameters,
      std::shared_ptr<memory::MemoryPool> _writerPool,
      std::shared_ptr<memory::MemoryPool> _sinkPool,
      std::shared_ptr<memory::MemoryPool> _sortPool)
      : writerParameters(std::move(parameters)),
        nonReclaimableSectionHolder(new tsan_atomic<bool>(false)),
        spillStats(
            std::make_unique<folly::Synchronized<velox::common::SpillStats>>()),
        writerPool(std::move(_writerPool)),
        sinkPool(std::move(_sinkPool)),
        sortPool(std::move(_sortPool)) {}

  const IcebergWriterParameters writerParameters;
  const std::unique_ptr<tsan_atomic<bool>> nonReclaimableSectionHolder;
  /// Collects the spill stats from sort writer if the spilling has been
  /// triggered.
  const std::unique_ptr<folly::Synchronized<velox::common::SpillStats>>
      spillStats;
  const std::shared_ptr<memory::MemoryPool> writerPool;
  const std::shared_ptr<memory::MemoryPool> sinkPool;
  const std::shared_ptr<memory::MemoryPool> sortPool;
  int64_t numWrittenRows = 0;
  int64_t inputSizeInBytes = 0;
};

/// Identifies a hive writer.
struct IcebergWriterId {
  std::optional<uint32_t> partitionId{std::nullopt};
  std::optional<uint32_t> bucketId{std::nullopt};

  IcebergWriterId() = default;

  IcebergWriterId(
      std::optional<uint32_t> _partitionId,
      std::optional<uint32_t> _bucketId = std::nullopt)
      : partitionId(_partitionId), bucketId(_bucketId) {}

  /// Returns the special writer id for the un-partitioned (and non-bucketed)
  /// table.
  static const IcebergWriterId& unpartitionedId();

  std::string toString() const;

  bool operator==(const IcebergWriterId& other) const {
    return std::tie(partitionId, bucketId) ==
        std::tie(other.partitionId, other.bucketId);
  }
};

struct IcebergWriterIdHasher {
  std::size_t operator()(const IcebergWriterId& id) const {
    return bits::hashMix(
        id.partitionId.value_or(std::numeric_limits<uint32_t>::max()),
        id.bucketId.value_or(std::numeric_limits<uint32_t>::max()));
  }
};

struct IcebergWriterIdEq {
  bool operator()(const IcebergWriterId& lhs, const IcebergWriterId& rhs) const {
    return lhs == rhs;
  }
};

class IcebergDataSink : public DataSink {
 public:
  /// The list of runtime stats reported by hive data sink
  static constexpr const char* kEarlyFlushedRawBytes = "earlyFlushedRawBytes";

  /// Defines the execution states of a hive data sink running internally.
  enum class State {
    /// The data sink accepts new append data in this state.
    kRunning = 0,
    /// The data sink flushes any buffered data to the underlying file writer
    /// but no more data can be appended.
    kFinishing = 1,
    /// The data sink is aborted on error and no more data can be appended.
    kAborted = 2,
    /// The data sink is closed on error and no more data can be appended.
    kClosed = 3
  };
  static std::string stateString(State state);

  IcebergDataSink(
      RowTypePtr inputType,
      std::shared_ptr<const IcebergInsertTableHandle> insertTableHandle,
      const ConnectorQueryCtx* connectorQueryCtx,
      CommitStrategy commitStrategy,
      const std::shared_ptr<const iceberg::IcebergConfig>& icebergConfig);

  IcebergDataSink(
      RowTypePtr inputType,
      std::shared_ptr<const IcebergInsertTableHandle> insertTableHandle,
      const ConnectorQueryCtx* connectorQueryCtx,
      CommitStrategy commitStrategy,
      const std::shared_ptr<const iceberg::IcebergConfig>& icebergConfig,
      const std::vector<column_index_t>& dataChannels);

  void appendData(RowVectorPtr input) override;

  bool finish() override;

  Stats stats() const override;

  std::vector<std::string> close() override;

  void abort() override;

  bool canReclaim() const;

 protected:
  // Validates the state transition from 'oldState' to 'newState'.
  void checkStateTransition(State oldState, State newState);

  void setState(State newState);

  virtual std::vector<std::string> commitMessage() const;

  class WriterReclaimer : public exec::MemoryReclaimer {
   public:
    static std::unique_ptr<memory::MemoryReclaimer> create(
        IcebergDataSink* dataSink,
        IcebergWriterInfo* writerInfo,
        io::IoStatistics* ioStats);

    bool reclaimableBytes(
        const memory::MemoryPool& pool,
        uint64_t& reclaimableBytes) const override;

    uint64_t reclaim(
        memory::MemoryPool* pool,
        uint64_t targetBytes,
        uint64_t maxWaitMs,
        memory::MemoryReclaimer::Stats& stats) override;

   private:
    WriterReclaimer(
        IcebergDataSink* dataSink,
        IcebergWriterInfo* writerInfo,
        io::IoStatistics* ioStats)
        : exec::MemoryReclaimer(0),
          dataSink_(dataSink),
          writerInfo_(writerInfo),
          ioStats_(ioStats) {
      VELOX_CHECK_NOT_NULL(dataSink_);
      VELOX_CHECK_NOT_NULL(writerInfo_);
      VELOX_CHECK_NOT_NULL(ioStats_);
    }

    IcebergDataSink* const dataSink_;
    IcebergWriterInfo* const writerInfo_;
    io::IoStatistics* const ioStats_;
  };

  FOLLY_ALWAYS_INLINE bool sortWrite() const {
    return !sortColumnIndices_.empty();
  }

  // Returns true if the table is partitioned.
  FOLLY_ALWAYS_INLINE bool isPartitioned() const {
    return partitionIdGenerator_ != nullptr;
  }

  // Returns true if the table is bucketed.
//  FOLLY_ALWAYS_INLINE bool isBucketed() const {
//    return bucketCount_ != 0;
//  }

  FOLLY_ALWAYS_INLINE bool isCommitRequired() const {
    return commitStrategy_ != CommitStrategy::kNoCommit;
  }

  std::shared_ptr<memory::MemoryPool> createWriterPool(
      const IcebergWriterId& writerId);

  void setMemoryReclaimers(
      IcebergWriterInfo* writerInfo,
      io::IoStatistics* ioStats);

  // Compute the partition id and bucket id for each row in 'input'.
  void computePartitionAndBucketIds(const RowVectorPtr& input);

  // Get the hive writer id corresponding to the row
  // from partitionIds and bucketIds.
  FOLLY_ALWAYS_INLINE IcebergWriterId getWriterId(size_t row) const;

  // Computes the number of input rows as well as the actual input row indices
  // to each corresponding (bucketed) partition based on the partition and
  // bucket ids calculated by 'computePartitionAndBucketIds'. The function also
  // ensures that there is a writer created for each (bucketed) partition.
  virtual void splitInputRowsAndEnsureWriters(RowVectorPtr input);

  // Makes sure the writer is created for the given writer id. The function
  // returns the corresponding index in 'writers_'.
  uint32_t ensureWriter(const IcebergWriterId& id);

  // Appends a new writer for the given 'id'. The function returns the index of
  // the newly created writer in 'writers_'.
  uint32_t appendWriter(const IcebergWriterId& id);

  virtual std::optional<std::string> getPartitionName(
      const IcebergWriterId& id) const;

  std::unique_ptr<facebook::velox::dwio::common::Writer>
  maybeCreateBucketSortWriter(
      std::unique_ptr<facebook::velox::dwio::common::Writer> writer);

  std::string makePartitionDirectory(
      const std::string& tableDirectory,
      const std::optional<std::string>& partitionSubdirectory) const;

  void
  updatePartitionRows(uint32_t index, vector_size_t numRows, vector_size_t row);

  void extendBuffersForPartitionedTables();

  IcebergWriterParameters getWriterParameters(
      const std::optional<std::string>& partition,
      std::optional<uint32_t> bucketId) const;

  // Gets write and target file names for a writer based on the table commit
  // strategy as well as table partitioned type. If commit is not required, the
  // write file and target file has the same name. If not, add a temp file
  // prefix to the target file for write file name. The coordinator (or driver
  // for Presto on spark) will rename the write file to target file to commit
  // the table write when update the metadata store. If it is a bucketed table,
  // the file name encodes the corresponding bucket id.
  std::pair<std::string, std::string> getWriterFileNames(
      std::optional<uint32_t> bucketId) const;

  IcebergWriterParameters::UpdateMode getUpdateMode() const;

  FOLLY_ALWAYS_INLINE void checkRunning() const {
    VELOX_CHECK_EQ(state_, State::kRunning, "Hive data sink is not running");
  }

  // Invoked to write 'input' to the specified file writer.
  void write(size_t index, RowVectorPtr input);

  void closeInternal();

  const RowTypePtr inputType_;
  const std::shared_ptr<const IcebergInsertTableHandle> insertTableHandle_;
  const ConnectorQueryCtx* const connectorQueryCtx_;
  const CommitStrategy commitStrategy_;
  const std::shared_ptr<const iceberg::IcebergConfig> icebergConfig_;
  const IcebergWriterParameters::UpdateMode updateMode_;
  const uint32_t maxOpenWriters_;
  const std::vector<column_index_t> partitionChannels_;
  const std::unique_ptr<PartitionIdGenerator> partitionIdGenerator_;
  // Indices of dataChannel are stored in ascending order
  const std::vector<column_index_t> dataChannels_;
//  const int32_t bucketCount_{0};
//  const std::unique_ptr<core::PartitionFunction> bucketFunction_;
  const std::shared_ptr<dwio::common::WriterFactory> writerFactory_;
  const velox::common::SpillConfig* const spillConfig_;
  const uint64_t sortWriterFinishTimeSliceLimitMs_{0};

  std::vector<column_index_t> sortColumnIndices_;
  std::vector<CompareFlags> sortCompareFlags_;

  State state_{State::kRunning};

  tsan_atomic<bool> nonReclaimableSection_{false};

  // The map from writer id to the writer index in 'writers_' and 'writerInfo_'.
  folly::F14FastMap<IcebergWriterId, uint32_t, IcebergWriterIdHasher, IcebergWriterIdEq>
      writerIndexMap_;

  // Below are structures for partitions from all inputs. writerInfo_ and
  // writers_ are both indexed by partitionId.
  std::vector<std::shared_ptr<IcebergWriterInfo>> writerInfo_;
  std::vector<std::unique_ptr<dwio::common::Writer>> writers_;
  // IO statistics collected for each writer.
  std::vector<std::shared_ptr<io::IoStatistics>> ioStats_;

  // Below are structures updated when processing current input. partitionIds_
  // are indexed by the row of input_. partitionRows_, rawPartitionRows_ and
  // partitionSizes_ are indexed by partitionId.
  raw_vector<uint64_t> partitionIds_;
  std::vector<BufferPtr> partitionRows_;
  std::vector<vector_size_t*> rawPartitionRows_;
  std::vector<vector_size_t> partitionSizes_;

  // Reusable buffers for bucket id calculations.
  std::vector<uint32_t> bucketIds_;

  // Strategy for naming writer files
  std::shared_ptr<const FileNameGenerator> fileNameGenerator_;
};

FOLLY_ALWAYS_INLINE std::ostream& operator<<(
    std::ostream& os,
    IcebergDataSink::State state) {
  os << IcebergDataSink::stateString(state);
  return os;
}
} // namespace facebook::velox::connector::lakehouse::iceberg

template <>
struct fmt::formatter<
    facebook::velox::connector::lakehouse::iceberg::IcebergDataSink::State>
    : formatter<std::string> {
  auto format(
      facebook::velox::connector::lakehouse::iceberg::IcebergDataSink::State s,
      format_context& ctx) const {
    return formatter<std::string>::format(
        facebook::velox::connector::lakehouse::iceberg::IcebergDataSink::stateString(s),
        ctx);
  }
};

template <>
struct fmt::formatter<
    facebook::velox::connector::lakehouse::iceberg::LocationHandle::TableType>
    : formatter<int> {
  auto format(
      facebook::velox::connector::lakehouse::iceberg::LocationHandle::TableType
          s,
      format_context& ctx) const {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};
