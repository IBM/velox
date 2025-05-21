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

#include "velox/common/config/Config.h"

namespace facebook::velox::connector::hive_common {

class ConnectorConfig {
 public:
  /// The GCS storage endpoint server.
  static constexpr const char* kGcsEndpoint = "hive.gcs.endpoint";

  /// The GCS service account configuration JSON key file.
  static constexpr const char* kGcsCredentialsPath =
      "hive.gcs.json-key-file-path";

  /// The GCS maximum retry counter of transient errors.
  static constexpr const char* kGcsMaxRetryCount = "hive.gcs.max-retry-count";

  /// The GCS maximum time allowed to retry transient errors.
  static constexpr const char* kGcsMaxRetryTime = "hive.gcs.max-retry-time";

  /// Maps table field names to file field names using names, not indices.
  // TODO: remove hive_orc_use_column_names since it doesn't exist in presto,
  // right now this is only used for testing.
  // TODO: remove hive. prefix
  static constexpr const char* kOrcUseColumnNames = "hive.orc.use-column-names";
  static constexpr const char* kOrcUseColumnNamesSession =
      "hive_orc_use_column_names";

  /// Maps table field names to file field names using names, not indices.
  static constexpr const char* kParquetUseColumnNames =
      "hive.parquet.use-column-names";
  static constexpr const char* kParquetUseColumnNamesSession =
      "parquet_use_column_names";

  /// Reads the source file column name as lower case.
  static constexpr const char* kFileColumnNamesReadAsLowerCase =
      "file-column-names-read-as-lower-case";
  static constexpr const char* kFileColumnNamesReadAsLowerCaseSession =
      "file_column_names_read_as_lower_case";

  static constexpr const char* kPartitionPathAsLowerCaseSession =
      "partition_path_as_lower_case";

  static constexpr const char* kAllowNullPartitionKeys =
      "allow-null-partition-keys";
  static constexpr const char* kAllowNullPartitionKeysSession =
      "allow_null_partition_keys";

  //  static constexpr const char* kIgnoreMissingFilesSession =
  //      "ignore_missing_files";

  /// The max coalesce bytes for a request.
  static constexpr const char* kMaxCoalescedBytes = "max-coalesced-bytes";
  static constexpr const char* kMaxCoalescedBytesSession =
      "max-coalesced-bytes";

  /// The max merge distance to combine read requests.
  /// Note: The session property name differs from the constant name for
  /// backward compatibility with Presto.
  static constexpr const char* kMaxCoalescedDistance = "max-coalesced-distance";
  static constexpr const char* kMaxCoalescedDistanceSession =
      "orc_max_merge_distance";

  /// The number of prefetch rowgroups
  static constexpr const char* kPrefetchRowGroups = "prefetch-rowgroups";

  /// The total size in bytes for a direct coalesce request. Up to 8MB load
  /// quantum size is supported when SSD cache is enabled.
  static constexpr const char* kLoadQuantum = "load-quantum";
  static constexpr const char* kLoadQuantumSession = "load-quantum";

  /// Maximum number of entries in the file handle cache.
  static constexpr const char* kNumCacheFileHandles = "num_cached_file_handles";

  /// Expiration time in ms for a file handle in the cache. A value of 0
  /// means cache will not evict the handle after kFileHandleExprationDurationMs
  /// has passed.
  static constexpr const char* kFileHandleExpirationDurationMs =
      "file-handle-expiration-duration-ms";

  /// Enable file handle cache.
  static constexpr const char* kEnableFileHandleCache =
      "file-handle-cache-enabled";

  /// The size in bytes to be fetched with Meta data together, used when the
  /// data after meta data will be used later. Optimization to decrease small IO
  /// request
  static constexpr const char* kFooterEstimatedSize = "footer-estimated-size";

  /// The threshold of file size in bytes when the whole file is fetched with
  /// meta data together. Optimization to decrease the small IO requests
  static constexpr const char* kFilePreloadThreshold = "file-preload-threshold";

  /// Config used to create write files. This config is provided to underlying
  /// file system through hive connector and data sink. The config is free form.
  /// The form should be defined by the underlying file system.
  static constexpr const char* kWriteFileCreateConfig =
      "hive.write_file_create_config";

  // The unit for reading timestamps from files.
  static constexpr const char* kReadTimestampUnit =
      "hive.reader.timestamp-unit";
  static constexpr const char* kReadTimestampUnitSession =
      "hive.reader.timestamp_unit";

  static constexpr const char* kReadTimestampPartitionValueAsLocalTime =
      "hive.reader.timestamp-partition-value-as-local-time";
  static constexpr const char* kReadTimestampPartitionValueAsLocalTimeSession =
      "hive.reader.timestamp_partition_value_as_local_time";

  static constexpr const char* kReadStatsBasedFilterReorderDisabled =
      "stats-based-filter-reorder-disabled";
  static constexpr const char* kReadStatsBasedFilterReorderDisabledSession =
      "stats_based_filter_reorder_disabled";

  template <typename T>
  T* as() {
    static_assert(std::is_base_of_v<ConnectorConfig, T>);
    return dynamic_cast<T*>(this);
  }

  template <typename T>
  const T* as() const {
    static_assert(std::is_base_of_v<ConnectorConfig, T>);
    return dynamic_cast<const T*>(this);
  }

  std::string gcsEndpoint() const;

  std::string gcsCredentialsPath() const;

  std::optional<int> gcsMaxRetryCount() const;

  std::optional<std::string> gcsMaxRetryTime() const;

  bool isOrcUseColumnNames(const velox::config::ConfigBase* session) const;

  bool isParquetUseColumnNames(const velox::config::ConfigBase* session) const;

  bool isFileColumnNamesReadAsLowerCase(
      const velox::config::ConfigBase* session) const;

  bool isPartitionPathAsLowerCase(
      const velox::config::ConfigBase* session) const;

  bool allowNullPartitionKeys(const velox::config::ConfigBase* session) const;

  //  bool ignoreMissingFiles(const velox::config::ConfigBase* session) const;

  int64_t maxCoalescedBytes(const velox::config::ConfigBase* session) const;

  int32_t maxCoalescedDistanceBytes(
      const velox::config::ConfigBase* session) const;

  int32_t prefetchRowGroups() const;

  int32_t loadQuantum(const velox::config::ConfigBase* session) const;

  int32_t numCacheFileHandles() const;

  uint64_t fileHandleExpirationDurationMs() const;

  bool isFileHandleCacheEnabled() const;

  uint64_t fileWriterFlushThresholdBytes() const;

  // Used by spiller
  std::string writeFileCreateConfig() const;

  //  uint32_t sortWriterMaxOutputRows(const velox::config::ConfigBase* session)
  //  const;
  //
  //  uint64_t sortWriterMaxOutputBytes(const velox::config::ConfigBase*
  //  session) const;
  //
  //  uint64_t sortWriterFinishTimeSliceLimitMs(
  //      const velox::config::ConfigBase* session) const;

  uint64_t footerEstimatedSize() const;

  uint64_t filePreloadThreshold() const;

  // Returns the timestamp unit used when reading timestamps from files.
  uint8_t readTimestampUnit(const velox::config::ConfigBase* session) const;

  // Whether to read timestamp partition value as local time. If false, read as
  // UTC.
  bool readTimestampPartitionValueAsLocalTime(
      const velox::config::ConfigBase* session) const;

  /// Returns true if the stats based filter reorder for read is disabled.
  bool readStatsBasedFilterReorderDisabled(
      const velox::config::ConfigBase* session) const;

  //  /// Returns the file system path containing local data. If non-empty,
  //  /// initializes LocalHiveConnectorMetadata to provide metadata for the
  //  tables
  //  /// in the directory.
  //  std::string hiveLocalDataPath() const;
  //
  //  /// Returns the name of the file format to use in interpreting the
  //  contents of
  //  /// hiveLocalDataPath().
  //  std::string hiveLocalFileFormat() const;

  ConnectorConfig(std::shared_ptr<const velox::config::ConfigBase> config) {
    VELOX_CHECK_NOT_NULL(
        config, "Config is null for HiveConfig initialization");
    config_ = std::move(config);
    // TODO: add sanity check
  }

  const std::shared_ptr<const velox::config::ConfigBase>& config() const {
    return config_;
  }

 protected:
  std::shared_ptr<const velox::config::ConfigBase> config_;
};

} // namespace facebook::velox::connector::hive_common
