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

#include "velox/connectors/hive_common/ConnectorConfig.h"

#include "velox/common/config/Config.h"

namespace facebook::velox::connector::hive_common {

std::string ConnectorConfig::gcsEndpoint() const {
  return config_->get<std::string>(kGcsEndpoint, std::string(""));
}

std::string ConnectorConfig::gcsCredentialsPath() const {
  return config_->get<std::string>(kGcsCredentialsPath, std::string(""));
}

std::optional<int> ConnectorConfig::gcsMaxRetryCount() const {
  return static_cast<std::optional<int>>(config_->get<int>(kGcsMaxRetryCount));
}

std::optional<std::string> ConnectorConfig::gcsMaxRetryTime() const {
  return static_cast<std::optional<std::string>>(
      config_->get<std::string>(kGcsMaxRetryTime));
}

bool ConnectorConfig::isOrcUseColumnNames(
    const config::ConfigBase* session) const {
  return session->get<bool>(
      kOrcUseColumnNamesSession, config_->get<bool>(kOrcUseColumnNames, false));
}

bool ConnectorConfig::isParquetUseColumnNames(
    const config::ConfigBase* session) const {
  return session->get<bool>(
      kParquetUseColumnNamesSession,
      config_->get<bool>(kParquetUseColumnNames, false));
}

bool ConnectorConfig::isFileColumnNamesReadAsLowerCase(
    const config::ConfigBase* session) const {
  return session->get<bool>(
      kFileColumnNamesReadAsLowerCaseSession,
      config_->get<bool>(kFileColumnNamesReadAsLowerCase, false));
}

bool ConnectorConfig::isPartitionPathAsLowerCase(
    const config::ConfigBase* session) const {
  return session->get<bool>(kPartitionPathAsLowerCaseSession, true);
}

bool ConnectorConfig::allowNullPartitionKeys(
    const config::ConfigBase* session) const {
  return session->get<bool>(
      kAllowNullPartitionKeysSession,
      config_->get<bool>(kAllowNullPartitionKeys, true));
}

int64_t ConnectorConfig::maxCoalescedBytes(
    const config::ConfigBase* session) const {
  return session->get<int64_t>(
      kMaxCoalescedBytesSession,
      config_->get<int64_t>(kMaxCoalescedBytes, 128 << 20)); // 128MB
}

int32_t ConnectorConfig::maxCoalescedDistanceBytes(
    const config::ConfigBase* session) const {
  const auto distance = config::toCapacity(
      session->get<std::string>(
          kMaxCoalescedDistanceSession,
          config_->get<std::string>(kMaxCoalescedDistance, "512kB")),
      config::CapacityUnit::BYTE);
  VELOX_USER_CHECK_LE(
      distance,
      std::numeric_limits<int32_t>::max(),
      "The max merge distance to combine read requests must be less than 2GB."
      " Got {} bytes.",
      distance);
  return int32_t(distance);
}

int32_t ConnectorConfig::prefetchRowGroups() const {
  return config_->get<int32_t>(kPrefetchRowGroups, 1);
}

int32_t ConnectorConfig::loadQuantum(const config::ConfigBase* session) const {
  return session->get<int32_t>(
      kLoadQuantumSession, config_->get<int32_t>(kLoadQuantum, 8 << 20));
}

int32_t ConnectorConfig::numCacheFileHandles() const {
  return config_->get<int32_t>(kNumCacheFileHandles, 20'000);
}

uint64_t ConnectorConfig::fileHandleExpirationDurationMs() const {
  return config_->get<uint64_t>(kFileHandleExpirationDurationMs, 0);
}

bool ConnectorConfig::isFileHandleCacheEnabled() const {
  return config_->get<bool>(kEnableFileHandleCache, true);
}

std::string ConnectorConfig::writeFileCreateConfig() const {
  return config_->get<std::string>(kWriteFileCreateConfig, "");
}

uint64_t ConnectorConfig::footerEstimatedSize() const {
  return config_->get<uint64_t>(kFooterEstimatedSize, 256UL << 10);
}

uint64_t ConnectorConfig::filePreloadThreshold() const {
  return config_->get<uint64_t>(kFilePreloadThreshold, 8UL << 20);
}

uint8_t ConnectorConfig::readTimestampUnit(
    const config::ConfigBase* session) const {
  const auto unit = session->get<uint8_t>(
      kReadTimestampUnitSession,
      config_->get<uint8_t>(kReadTimestampUnit, 3 /*milli*/));
  VELOX_CHECK(
      unit == 3 || unit == 6 /*micro*/ || unit == 9 /*nano*/,
      "Invalid timestamp unit.");
  return unit;
}

bool ConnectorConfig::readTimestampPartitionValueAsLocalTime(
    const config::ConfigBase* session) const {
  return session->get<bool>(
      kReadTimestampPartitionValueAsLocalTimeSession,
      config_->get<bool>(kReadTimestampPartitionValueAsLocalTime, true));
}

bool ConnectorConfig::readStatsBasedFilterReorderDisabled(
    const config::ConfigBase* session) const {
  return session->get<bool>(
      kReadStatsBasedFilterReorderDisabledSession,
      config_->get<bool>(kReadStatsBasedFilterReorderDisabled, false));
}

} // namespace facebook::velox::connector::hive_common
