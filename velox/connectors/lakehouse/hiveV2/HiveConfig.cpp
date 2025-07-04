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

#include "HiveConfig.h"

#include "velox/core/QueryConfig.h"

#include <boost/algorithm/string.hpp>

namespace facebook::velox::connector::lakehouse::hive {

namespace {

HiveConfig::InsertExistingPartitionsBehavior
stringToInsertExistingPartitionsBehavior(const std::string& strValue) {
  auto upperValue = boost::algorithm::to_upper_copy(strValue);
  if (upperValue == "ERROR") {
    return HiveConfig::InsertExistingPartitionsBehavior::kError;
  }
  if (upperValue == "OVERWRITE") {
    return HiveConfig::InsertExistingPartitionsBehavior::kOverwrite;
  }
  VELOX_UNSUPPORTED(
      "Unsupported insert existing partitions behavior: {}.", strValue);
}

} // namespace

// static
std::string HiveConfig::insertExistingPartitionsBehaviorString(
    InsertExistingPartitionsBehavior behavior) {
  switch (behavior) {
    case InsertExistingPartitionsBehavior::kError:
      return "ERROR";
    case InsertExistingPartitionsBehavior::kOverwrite:
      return "OVERWRITE";
    default:
      return fmt::format("UNKNOWN BEHAVIOR {}", static_cast<int>(behavior));
  }
}

HiveConfig::InsertExistingPartitionsBehavior
HiveConfig::insertExistingPartitionsBehavior(
    const config::ConfigBase* session) const {
  return stringToInsertExistingPartitionsBehavior(session->get<std::string>(
      kInsertExistingPartitionsBehaviorSession,
      config_->get<std::string>(kInsertExistingPartitionsBehavior, "ERROR")));
}

uint32_t HiveConfig::maxPartitionsPerWriters(
    const config::ConfigBase* session) const {
  return session->get<uint32_t>(
      kMaxPartitionsPerWritersSession,
      config_->get<uint32_t>(kMaxPartitionsPerWriters, 128));
}

bool HiveConfig::immutablePartitions() const {
  return config_->get<bool>(kImmutablePartitions, false);
}

bool HiveConfig::ignoreMissingFiles(const config::ConfigBase* session) const {
  return session->get<bool>(kIgnoreMissingFilesSession, false);
}

uint32_t HiveConfig::sortWriterMaxOutputRows(
    const config::ConfigBase* session) const {
  return session->get<uint32_t>(
      kSortWriterMaxOutputRowsSession,
      config_->get<uint32_t>(kSortWriterMaxOutputRows, 1024));
}

uint64_t HiveConfig::sortWriterMaxOutputBytes(
    const config::ConfigBase* session) const {
  return config::toCapacity(
      session->get<std::string>(
          kSortWriterMaxOutputBytesSession,
          config_->get<std::string>(kSortWriterMaxOutputBytes, "10MB")),
      config::CapacityUnit::BYTE);
}

uint64_t HiveConfig::sortWriterFinishTimeSliceLimitMs(
    const config::ConfigBase* session) const {
  return session->get<uint64_t>(
      kSortWriterFinishTimeSliceLimitMsSession,
      config_->get<uint64_t>(kSortWriterFinishTimeSliceLimitMs, 5'000));
}

std::string HiveConfig::hiveLocalDataPath() const {
  return config_->get<std::string>(kLocalDataPath, "");
}

std::string HiveConfig::hiveLocalFileFormat() const {
  return config_->get<std::string>(kLocalFileFormat, "");
}

} // namespace facebook::velox::connector::lakehouse::hive
