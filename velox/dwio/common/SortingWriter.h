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

#include "velox/dwio/common/Writer.h"
#include "velox/exec/MemoryReclaimer.h"
#include "velox/exec/SortBuffer.h"

namespace facebook::velox::dwio::common {

/// Sorting Writer object is used to write sorted data into a single file.
class SortingWriter : public Writer {
 public:
  /// Starts a new output file when the current one is full, so the sorted
  /// output spans several files. Each file then holds a contiguous range of the
  /// sort order.
  class FileRotator {
   public:
    virtual ~FileRotator() = default;

    /// Returns true when the output file of 'writer' has reached its target
    /// size.
    virtual bool isFileFull(const Writer& writer) = 0;

    /// Closes 'fullWriter', which wrote 'numRows' rows, and returns the writer
    /// for the next file.
    virtual std::unique_ptr<Writer> rotate(
        std::unique_ptr<Writer> fullWriter,
        uint64_t numRows) = 0;
  };

  /// @param numSortOnlyColumns Number of trailing input columns that only serve
  /// as sort keys. 'sortBuffer' sorts on them, and they are dropped before the
  /// sorted rows reach 'writer'.
  /// @param fileRotator Splits the sorted output across files. Null writes all
  /// of it to 'writer'.
  SortingWriter(
      std::unique_ptr<Writer> writer,
      std::unique_ptr<exec::SortBuffer> sortBuffer,
      vector_size_t maxOutputRowsConfig,
      uint64_t maxOutputBytesConfig,
      uint64_t outputTimeSliceLimitMs,
      column_index_t numSortOnlyColumns,
      std::unique_ptr<FileRotator> fileRotator);

  ~SortingWriter() override;

  void write(const VectorPtr& data) override;

  bool finish() override;

  /// No action because we need to accumulate all data and sort before data can
  /// be flushed.
  void flush() override;

  /// Closes the writer. Returns file metadata, or null if no metadata is
  /// available (e.g. for an empty file).
  std::unique_ptr<FileMetadata> close() override;

  folly::F14FastMap<std::string, RuntimeMetric> runtimeStats() const override {
    return outputWriter_->runtimeStats();
  }

  void abort() override;

 private:
  class MemoryReclaimer : public exec::MemoryReclaimer {
   public:
    static std::unique_ptr<memory::MemoryReclaimer> create(
        SortingWriter* writer);

    bool reclaimableBytes(
        const memory::MemoryPool& pool,
        uint64_t& reclaimableBytes) const override;

    uint64_t reclaim(
        memory::MemoryPool* pool,
        uint64_t targetBytes,
        uint64_t maxWaitMs,
        memory::MemoryReclaimer::Stats& stats) override;

   private:
    MemoryReclaimer(SortingWriter* writer)
        : exec::MemoryReclaimer(0),
          writer_(writer),
          canReclaim_(writer_->sortBuffer_->canSpill()) {}

    SortingWriter* const writer_;
    const bool canReclaim_;
  };

  bool canReclaim() const;

  uint64_t reclaim(uint64_t targetBytes, memory::MemoryReclaimer::Stats& stats);

  vector_size_t outputBatchRows();

  // Drops the sort-only columns from 'sorted'.
  RowVectorPtr dropSortOnlyColumns(const RowVectorPtr& sorted) const;

  // Writes the current output file. Replaced when 'fileRotator_' starts a new
  // file.
  std::unique_ptr<Writer> outputWriter_;
  const vector_size_t maxOutputRowsConfig_;
  const uint64_t maxOutputBytesConfig_;
  const uint64_t finishTimeSliceLimitMs_;
  const column_index_t numSortOnlyColumns_;
  const std::unique_ptr<FileRotator> fileRotator_;
  memory::MemoryPool* const sortPool_;
  const bool canReclaim_;

  std::unique_ptr<exec::SortBuffer> sortBuffer_;
  // Rows written to the current output file.
  uint64_t numRowsInFile_{0};
};

} // namespace facebook::velox::dwio::common
