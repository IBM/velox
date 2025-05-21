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

#include "velox/common/base/Exceptions.h"
#include "velox/connectors/hive_common/ConnectorConfig.h"

#include <optional>
#include <string>

namespace facebook::velox::config {
class ConfigBase;
}

namespace facebook::velox::connector::hiveV2 {

/// Hive connector configs.
class HiveConfig : public hive_common::ConnectorConfig {
 public:
  enum class InsertExistingPartitionsBehavior {
    kError,
    kOverwrite,
  };

  static std::string insertExistingPartitionsBehaviorString(
      InsertExistingPartitionsBehavior behavior);
  /// Behavior on insert into existing partitions.
  static constexpr const char* kInsertExistingPartitionsBehaviorSession =
      "insert_existing_partitions_behavior";
  static constexpr const char* kInsertExistingPartitionsBehavior =
      "insert-existing-partitions-behavior";

  /// Maximum number of (bucketed) partitions per a single table writer
  /// instance.
  static constexpr const char* kMaxPartitionsPerWriters =
      "max-partitions-per-writers";
  static constexpr const char* kMaxPartitionsPerWritersSession =
      "max_partitions_per_writers";

  /// Whether new data can be inserted into an unpartition table.
  /// Velox currently does not support appending data to existing partitions.
  static constexpr const char* kImmutablePartitions =
      "hive.immutable-partitions";

  static constexpr const char* kIgnoreMissingFilesSession =
      "ignore_missing_files";

  /// Maximum number of rows for sort writer in one batch of output.
  static constexpr const char* kSortWriterMaxOutputRows =
      "sort-writer-max-output-rows";
  static constexpr const char* kSortWriterMaxOutputRowsSession =
      "sort_writer_max_output_rows";

  /// Maximum bytes for sort writer in one batch of output.
  static constexpr const char* kSortWriterMaxOutputBytes =
      "sort-writer-max-output-bytes";
  static constexpr const char* kSortWriterMaxOutputBytesSession =
      "sort_writer_max_output_bytes";

  /// Sort Writer will exit finish() method after this many milliseconds even if
  /// it has not completed its work yet. Zero means no time limit.
  static constexpr const char* kSortWriterFinishTimeSliceLimitMs =
      "sort-writer_finish_time_slice_limit_ms";
  static constexpr const char* kSortWriterFinishTimeSliceLimitMsSession =
      "sort_writer_finish_time_slice_limit_ms";

  static constexpr const char* kLocalDataPath = "hive_local_data_path";
  static constexpr const char* kLocalFileFormat = "hive_local_file_format";

  InsertExistingPartitionsBehavior insertExistingPartitionsBehavior(
      const config::ConfigBase* session) const;

  uint32_t maxPartitionsPerWriters(const config::ConfigBase* session) const;

  bool immutablePartitions() const;

  bool ignoreMissingFiles(const config::ConfigBase* session) const;

  uint32_t sortWriterMaxOutputRows(const config::ConfigBase* session) const;

  uint64_t sortWriterMaxOutputBytes(const config::ConfigBase* session) const;

  uint64_t sortWriterFinishTimeSliceLimitMs(
      const config::ConfigBase* session) const;

  /// Returns the file system path containing local data. If non-empty,
  /// initializes LocalHiveConnectorMetadata to provide metadata for the tables
  /// in the directory.
  std::string hiveLocalDataPath() const;

  /// Returns the name of the file format to use in interpreting the contents of
  /// hiveLocalDataPath().
  std::string hiveLocalFileFormat() const;

  HiveConfig(std::shared_ptr<const config::ConfigBase> config)
      : hive_common::ConnectorConfig(config) {}
};

} // namespace facebook::velox::connector::hiveV2
