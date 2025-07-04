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

#include "velox/common/base/RandomUtil.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/lakehouse/base/ConnectorConfigBase.h"
#include "velox/connectors/lakehouse/base/ConnectorSplitBase.h"
#include "velox/connectors/lakehouse/base/FileHandle.h"
#include "velox/connectors/lakehouse/base/TableHandleBase.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/Reader.h"
#include "velox/type/Type.h"

#include <random>
#include <shared_mutex>
#include <unordered_map>

namespace facebook::velox::connector::lakehouse::base {

class SplitReaderBase {
 public:
  SplitReaderBase(
      const std::shared_ptr<const ConnectorSplitBase>& split,
      const std::shared_ptr<const TableHandleBase>& tableHandle,
      const std::unordered_map<std::string, std::shared_ptr<ColumnHandleBase>>*
          partitionColumnHandles,
      const ConnectorQueryCtx* connectorQueryCtx,
      const std::shared_ptr<const ConnectorConfigBase>& ConnectorConfigBase,
      const RowTypePtr& readerOutputType,
      const std::shared_ptr<io::IoStatistics>& ioStats,
      const std::shared_ptr<filesystems::File::IoStats>& fsStats,
      FileHandleFactory* fileHandleFactory,
      folly::Executor* executor,
      const std::shared_ptr<velox::common::ScanSpec>& scanSpec);

  virtual ~SplitReaderBase() = default;

  void configureReaderOptions(
      std::shared_ptr<random::RandomSkipTracker> randomSkip);

  /// This function is used by different table formats like Iceberg and Hudi to
  /// do additional preparations before reading the split, e.g. Open delete
  /// files or log files, and add column adapatations for metadata columns. It
  /// would be called only once per incoming split
  virtual void prepareSplit(
      std::shared_ptr<velox::common::MetadataFilter> metadataFilter,
      dwio::common::RuntimeStatistics& runtimeStats);

  virtual uint64_t next(uint64_t size, VectorPtr& output);

  void resetFilterCaches();

  bool emptySplit() const;

  void resetSplit();

  int64_t estimatedRowSize() const;

  void updateRuntimeStats(dwio::common::RuntimeStatistics& stats) const;

  bool allPrefetchIssued() const;

  void setConnectorQueryCtx(const ConnectorQueryCtx* connectorQueryCtx);

  const RowTypePtr& readerOutputType() const {
    return readerOutputType_;
  }

  std::string toString() const;

 protected:
  /// Create the dwio::common::Reader object baseReader_, which will be
  /// used to read the data file's metadata and schema
  void createReader();

  // Adjust the scan spec according to the current split, then return the
  // adapted row type.
  RowTypePtr getAdaptedRowType() const;

  /// Check if the split_ is empty. The split is considered empty when
  ///   1) The data file is missing but the user chooses to ignore it
  ///   2) The file does not contain any rows
  ///   3) The data in the file does not pass the filters. The test is based on
  ///      the file metadata and partition key values
  /// This function needs to be called after baseReader_ is created.
  bool checkIfSplitIsEmpty(dwio::common::RuntimeStatistics& runtimeStats);

  // Check if the filters pass on the column statistics.  When delta update is
  // present, the corresonding filter should be disabled before calling this
  // function.
  virtual bool filterSplit(
      dwio::common::RuntimeStatistics& runtimeStats) const {
    VELOX_UNREACHABLE();
  }

  /// Create the dwio::common::RowReader object baseRowReader_, which owns
  /// the ColumnReaders that will be used to read the data
  void createRowReader(
      std::shared_ptr<velox::common::MetadataFilter> metadataFilter,
      RowTypePtr rowType);

  /// Different table formats may have different meatadata columns.
  /// This function will be used to update the scanSpec for these columns.
  virtual std::vector<TypePtr> adaptColumns(
      const RowTypePtr& fileType,
      const std::shared_ptr<const velox::RowType>& tableSchema) const;

  std::string toStringBase(const std::string& className) const;

  std::shared_ptr<const ConnectorSplitBase> split_;
  const std::shared_ptr<const TableHandleBase> tableHandle_;
  const std::unordered_map<std::string, std::shared_ptr<ColumnHandleBase>>*
      partitionColumnHandles_;
  const ConnectorQueryCtx* connectorQueryCtx_;
  const std::shared_ptr<const ConnectorConfigBase> ConnectorConfigBase_;

  RowTypePtr readerOutputType_;
  const std::shared_ptr<io::IoStatistics> ioStats_;
  const std::shared_ptr<filesystems::File::IoStats> fsStats_;
  FileHandleFactory* const fileHandleFactory_;
  folly::Executor* const executor_;
  memory::MemoryPool* const pool_;

  std::shared_ptr<velox::common::ScanSpec> scanSpec_;
  std::unique_ptr<dwio::common::Reader> baseReader_;
  std::unique_ptr<dwio::common::RowReader> baseRowReader_;
  dwio::common::ReaderOptions baseReaderOpts_;
  dwio::common::RowReaderOptions baseRowReaderOpts_;
  bool emptySplit_;
};

} // namespace facebook::velox::connector::lakehouse::base
