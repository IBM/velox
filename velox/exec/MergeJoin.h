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
#include <folly/container/F14Map.h>

#include "velox/exec/MergeSource.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

/// The merge join operator assumes both streams, left (from addInput()) and
/// right (from rightSource), are sorted in ascending order on the join key.
///
/// It works by identifying and maintaining a window of rows with key matches
/// (leftMatch_ and rightMatch_), and outputting a cartesian product of each key
/// match. Since keys can span multiple vectors, multiple batches from either
/// side may need to be materialized and kept in memory. Therefore, the memory
/// requirement is proportional to the size of the longest key match. Once all
/// output for a particular key match is produced, the respective batches are
/// discarded.
///
/// Output is produced outputBatchSize_ rows at a time.
///
/// The merge join operator generally returns dictionaries which are wrapped
/// around input vectors. The output is aligned to left vectors, and since
/// dictionaries cannot wrap around more than one vector, at times merge join
/// may return fewer than outputBatchSize_ rows.
///
/// Dictionaries for right projections are optimistically created; we start by
/// wrapping the current right vector, but if the output happens to span more
/// than one right vector, it gets copied and flattened.
class MergeJoin : public Operator {
 public:
  MergeJoin(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::MergeJoinNode>& joinNode);

  void initialize() override;

  BlockingReason isBlocked(ContinueFuture* future) override;

  bool needsInput() const override;

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  bool isFinished() override;

  bool startDrain() override;

  void finishDrain() override;

  void close() override;

 private:
  // Sets up 'filter_' and related member variables.
  void initializeFilter(
      const core::TypedExprPtr& filter,
      const RowTypePtr& leftType,
      const RowTypePtr& rightType);

  // The handling of null rows on the right side for right and full type of
  // joins.
  RowVectorPtr handleRightSideNullRows();

  RowVectorPtr doGetOutput();

  static int32_t compare(
      const std::vector<column_index_t>& keys,
      const RowVectorPtr& batch,
      vector_size_t index,
      const std::vector<column_index_t>& otherKeys,
      const RowVectorPtr& otherBatch,
      vector_size_t otherIndex);

  // Compare rows on the left and right at index_ and rightIndex_ respectively.
  int32_t compare() const {
    return compare(
        leftKeyChannels_,
        input_,
        leftRowIndex_,
        rightKeyChannels_,
        rightInput_,
        rightRowIndex_);
  }

  // Compare two rows on the left: index_ and index.
  int32_t compareLeft(vector_size_t index) const {
    return compare(
        leftKeyChannels_,
        input_,
        leftRowIndex_,
        leftKeyChannels_,
        input_,
        index);
  }

  // Compare two rows on the right: rightIndex_ and index.
  int32_t compareRight(vector_size_t index) const {
    return compare(
        rightKeyChannels_,
        rightInput_,
        rightRowIndex_,
        rightKeyChannels_,
        rightInput_,
        index);
  }

  // Compare two rows from the left side.
  int32_t compareLeft(
      const RowVectorPtr& batch,
      vector_size_t index,
      const RowVectorPtr& otherBatch,
      vector_size_t otherIndex) const {
    return compare(
        leftKeyChannels_,
        batch,
        index,
        leftKeyChannels_,
        otherBatch,
        otherIndex);
  }

  // Compare two rows from the right side.
  int32_t compareRight(
      const RowVectorPtr& batch,
      vector_size_t index,
      const RowVectorPtr& otherBatch,
      vector_size_t otherIndex) const {
    return compare(
        rightKeyChannels_,
        batch,
        index,
        rightKeyChannels_,
        otherBatch,
        otherIndex);
  }

  bool rightHasNoInput() const {
    return noMoreRightInput_ || rightHasDrained_;
  }

  bool leftHasNoInput() const {
    return noMoreInput_ || leftHasDrained_;
  }

  // Describes a contiguous set of rows on the left or right side of the join
  // with all join keys being the same. The set of rows may span multiple
  // batches of input.
  struct Match {
    // One or more batches of inputs that contain rows with matching keys.
    std::vector<RowVectorPtr> inputs;

    // Row number in the first batch pointing to the first row with matching
    // keys.
    vector_size_t startRowIndex{0};

    // Row number in the last batch pointing to the row just past the row with
    // matching keys.
    vector_size_t endRowIndex{0};

    // True if all matching rows have been collected. False, if more batches
    // need to be processed to identify all matching rows.
    bool complete{false};

    // Identifies a particular row in a set rows with matching keys (Match).
    // Used to store a restart position for when output vector filled up before
    // the full set of matching rows was added. The next call to getOutput will
    // continue filling up next output batch from that place.
    struct Cursor {
      // Index of the batch.
      size_t batchIndex{0};

      // Row number in the batch specified by batchIndex.
      vector_size_t rowIndex{0};
    };

    // A position to continue producing output from. Set if complete set of
    // rows with matching keys didn't fit into output batch.
    std::optional<Cursor> cursor;

    // A convenience method to set or update 'cursor'.
    void setCursor(size_t batchIndex, vector_size_t rowIndex) {
      cursor = Cursor{batchIndex, rowIndex};
    }
  };

  // Given a partial set of rows with matching keys (match) finds all rows from
  // the start of the 'input' batch that also have matching keys. Updates
  // 'match' to include the newly identified rows. Returns true if found the
  // last matching row and set match.complete to true. If all rows in 'input'
  // have matching keys, adds 'input' to 'match' and returns false to ensure
  // that next batch of input is checked for more matching rows.
  bool findEndOfMatch(
      const RowVectorPtr& input,
      const std::vector<column_index_t>& keys,
      Match& match);

  // Ensures `output_` is ready to receive records via `addOutput()` or
  // `addOutputRowForLeftJoin()`. Initialize vectors using `outputBatchSize_`.
  // Returns true is the output_ needs to be returned/produced first, and false
  // in case it is ready to take records.
  bool prepareOutput(const RowVectorPtr& left, const RowVectorPtr& right);

  // Appends a cartesian product of the current set of matching rows, leftMatch_
  // x rightMatch_ for left join and rightMatch_ x leftMatch_ for right join, to
  // output_. Returns true if output_ is full. Sets leftMatchCursor_ and
  // rightMatchCursor_ if output_ filled up before all the rows were added.
  // Fills up output starting from leftMatchCursor_ and rightMatchCursor_
  // positions if these are set. Clears leftMatch_ and rightMatch_ if all rows
  // were added. Updates leftMatchCursor_ and rightMatchCursor_ if output_
  // filled up before all rows were added.
  bool addToOutput();

  // Appends the current set of matching rows, leftMatch_ x rightMatch_ for
  // left.
  bool addToOutputForLeftJoin();

  // Appends the current set of matching rows, rightMatch_ x leftMatch_ for
  // right.
  bool addToOutputForRightJoin();

  // Tries to add one row of output by writing to the indices of the output
  // dictionaries. By default, this operator returns dictionaries wrapped around
  // the input columns from the left and right. If `isRightFlattened_`, the
  // right side projections are copied to the output.
  //
  // If there is space in the output, advances outputSize_ and returns true.
  // Otherwise returns false and outputSize_ is unchanged.
  bool tryAddOutputRow(
      const RowVectorPtr& leftBatch,
      vector_size_t leftRow,
      const RowVectorPtr& rightBatch,
      vector_size_t rightRow,
      bool isRightJoinForFullOuter = false);

  // If the right side projected columns in the current output vector happen to
  // span more than one vector from the right side, they cannot be simply
  // wrapped in a dictionary and must be flattened.
  //
  // TODO: in theory they can be copied and turned into a dictionary, but this
  // logic is more involved.
  void flattenRightProjections();

  // Tries to add one row of output for a left-side row with no right-side
  // match. Copies values from the 'leftIndex' row of 'left' and fills in nulls
  // for columns that correspond to the right side.
  //
  // If there is space in the output, advances outputSize_ and leftRowIndex_,
  // and returns true. Otherwise returns false and outputSize_ and leftRowIndex_
  // are unchanged.
  bool tryAddOutputRowForLeftJoin();

  // Tries to add one row of output for a right-side row with no left-side
  // match. Copies values from the 'rightIndex' row of 'right' and fills in
  // nulls for columns that correspond to the right side.
  //
  // If there is space in the output, advances outputSize_ and rightRowIndex_,
  // and returns true. Otherwise returns false and outputSize_ and
  // rightRowIndex_ are unchanged.
  bool tryAddOutputRowForRightJoin();

  // If all rows from the current left batch have been processed.
  bool finishedLeftBatch() const {
    return leftRowIndex_ == input_->size();
  }

  // If all rows from the current right batch have been processed.
  bool finishedRightBatch() const {
    return rightRowIndex_ == rightInput_->size();
  }

  // Properly resizes and produces the current output vector if one is
  // available.
  RowVectorPtr produceOutput() {
    if (output_ == nullptr) {
      return nullptr;
    }
    output_->resize(outputSize_);
    return std::move(output_);
  }

  // Evaluates join filter on 'filterInput_' and returns 'output' that contains
  // a subset of rows on which the filter passed. Returns nullptr if no rows
  // passed the filter.
  RowVectorPtr applyFilter(const RowVectorPtr& output);

  // Evaluates 'filter_' on the specified rows of 'filterInput_' and decodes
  // the result using 'decodedFilterResult_'.
  void evaluateFilter(const SelectivityVector& rows);

  // An anti join is equivalent to a left join that retains only the rows from
  // the left side which do not have a corresponding match on the right side.
  // When an anti join includes a filter, it is processed using the applyFilter
  // method. For an anti join without a filter, we must specifically exclude
  // rows from the left side that have a match on the right.
  RowVectorPtr filterOutputForAntiJoin(const RowVectorPtr& output);

  // As we populate the results of the join, we track whether a given
  // output row is a result of a match between left and right sides or a miss.
  // We use JoinTracker::addMatch and addMiss methods for that.
  //
  // The semantic of the filter is to include at least one left side row in the
  // output after filters are applied. Therefore:
  //
  // 1. if left was a miss on the right side: just leave the current row as-is
  // without even evaluating the filter (it would have to be added even if
  // filters failed).
  //
  // 2. if left was a hit on the side: if at least one row from the key match
  // passes the filter, leave them as-is. If none passed, add a new row with
  // the right projections null (see `noMoreFilterResults()`).
  //
  // Specifically, once we have a batch of output, we evaluate the filter on a
  // subset of rows which correspond to matches between left and right sides.
  // There is no point evaluating filters on misses as these need to be
  // included in the output regardless of whether filter passes or fails.
  //
  // We also track blocks of consecutive output rows that correspond to the
  // same left-side row. If the filter passes on at least one row in such a
  // block, we keep the subset of passing rows. However, if the filter failed
  // on all rows in such a block, we add one of these rows back and update
  // build-side columns to null.
  struct JoinTracker {
    JoinTracker(vector_size_t numRows, memory::MemoryPool* pool)
        : matchingRows_{numRows, false} {
      leftRowNumbers_ = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
      rawLeftRowNumbers_ = leftRowNumbers_->asMutable<vector_size_t>();

      rightJoinRows_ = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
      rawRightJoinRows_ = rightJoinRows_->asMutable<bool>();

      auto tmp = AlignedBuffer::allocate<vector_size_t*>(numRows, pool);
    }

    // Records a row of output that corresponds to a match between a left-side
    // row and a right-side row. Assigns synthetic number to uniquely identify
    // the corresponding left-side row. The caller must call addMatch or
    // addMiss method for each row of output in order, starting with the first
    // row.
    void addMatch(
        const VectorPtr& vector,
        vector_size_t row,
        vector_size_t outputIndex,
        bool rightJoinForFullOuter = false) {
      matchingRows_.setValid(outputIndex, true);

      if (lastVector_ != vector || lastIndex_ != row) {
        // New left-side row.
        ++lastLeftRowNumber_;
        lastVector_ = vector;
        lastIndex_ = row;
      }

      rawLeftRowNumbers_[outputIndex] = lastLeftRowNumber_;
      rawRightJoinRows_[outputIndex] = rightJoinForFullOuter;
    }

    // Returns a subset of "match" rows in [0, numRows) range that were
    // recorded by addMatch.
    const SelectivityVector& matchingRows(vector_size_t numRows) {
      matchingRows_.setValidRange(numRows, matchingRows_.size(), false);
      matchingRows_.updateBounds();
      return matchingRows_;
    }

    // Records a row of output that corresponds to a left-side
    // row that has no match on the right-side. The caller must call addMatch
    // or addMiss method for each row of output in order, starting with the
    // first row.
    void addMiss(vector_size_t outputIndex) {
      matchingRows_.setValid(outputIndex, false);
      resetLastVector();
    }

    // Clear the left-side vector and index of the last added output row. The
    // left-side vector has been fully processed and is now available for
    // re-use, hence, need to make sure that new rows won't be confused with
    // the old ones.
    void resetLastVector() {
      lastVector_.reset();
      lastIndex_ = -1;
    }

    // Called for each row that the filter was evaluated on, in order, starting
    // with the first row. Calls 'onMiss' if the filter failed on all output
    // rows that correspond to a single left-side row. Use
    // 'noMoreFilterResults' to make sure 'onMiss' is called for the last
    // left-side row.
    template <typename TOnMiss, typename TOnMatch>
    void processFilterResult(
        vector_size_t outputIndex,
        bool passed,
        const TOnMiss& onMiss,
        const TOnMatch& onMatch) {
      const auto rowNumber = rawLeftRowNumbers_[outputIndex];
      if (currentLeftRowNumber_ != rowNumber) {
        if (currentRow_ != -1 && !currentRowPassed_) {
          onMiss(currentRow_, rawRightJoinRows_[currentRow_]);
        }
        currentRow_ = outputIndex;
        currentLeftRowNumber_ = rowNumber;
        currentRowPassed_ = false;
      } else {
        currentRow_ = outputIndex;
      }

      if (passed) {
        onMatch(outputIndex, /*firstMatch=*/!currentRowPassed_);
        currentRowPassed_ = true;
      }
    }

    // Returns whether `row` corresponds to the same left key as the last
    // left match evaluated.
    bool isCurrentLeftMatch(vector_size_t row) {
      return currentLeftRowNumber_ == rawLeftRowNumbers_[row];
    }

    // Called when all rows from the current output batch are processed and the
    // next batch of output will start with a new left-side row or there will
    // be no more batches. Calls 'onMiss' for the last left-side row if the
    // filter failed for all matches of that row.
    template <typename TOnMiss>
    void noMoreFilterResults(TOnMiss onMiss) {
      if (!currentRowPassed_ && currentRow_ >= 0) {
        onMiss(currentRow_, rawRightJoinRows_[currentRow_]);
      }

      currentRow_ = -1;
      currentRowPassed_ = false;
    }

    void reset();

    bool isRightJoinForFullOuter(vector_size_t row) {
      return rawRightJoinRows_[row];
    }

   private:
    // A subset of output rows where left side matched right side on the join
    // keys. Used in filter evaluation.
    SelectivityVector matchingRows_;

    // The left-side vector and index of the last added row. Used to identify
    // the end of a block of output rows that correspond to the same left-side
    // row.
    VectorPtr lastVector_{nullptr};
    vector_size_t lastIndex_{-1};

    // Synthetic numbers used to uniquely identify a left-side row. We cannot
    // use row number from the left-side vector because a given batch of output
    // may contains rows from multiple left-side batches. Only "match" rows
    // added via addMatch are being tracked. The values for "miss" rows are
    // not defined.
    BufferPtr leftRowNumbers_;
    vector_size_t* rawLeftRowNumbers_;

    BufferPtr rightJoinRows_;
    bool* rawRightJoinRows_;

    // Synthetic number assigned to the last added "match" row or zero if no row
    // has been added yet.
    vector_size_t lastLeftRowNumber_{0};

    // Output index of the last output row for which filter result was recorded.
    vector_size_t currentRow_{-1};

    // Synthetic number for the 'currentRow'.
    vector_size_t currentLeftRowNumber_{-1};

    // True if at least one row in a block of output rows corresponding a single
    // left-side row identified by 'currentRowNumber' passed the filter.
    bool currentRowPassed_{false};
  };

  /// Used to record both left and right join.
  std::optional<JoinTracker> joinTracker_{std::nullopt};

  // Indices buffer used by the output dictionaries. All projection from the
  // left share `leftIndices_`, and projections in the right share
  // `rightIndices_`.
  BufferPtr leftOutputIndices_;
  BufferPtr rightOutputIndices_;

  vector_size_t* rawLeftOutputIndices_;
  vector_size_t* rawRightOutputIndices_;

  // Stores the current left and right vectors being used by the output
  // dictionaries.
  RowVectorPtr currentLeft_;
  RowVectorPtr currentRight_;

  // If the right side projections have been flattened or they are still
  // dictionaries wrapped around the right side input.
  bool isRightFlattened_{false};

  // Maximum number of rows in the output batch.
  const vector_size_t outputBatchSize_;

  // Type of join.
  const core::JoinType joinType_;

  // Number of join keys.
  const size_t numKeys_;

  const core::PlanNodeId rightNodeId_;

  // The cached merge join plan node used to initialize this operator after the
  // driver has started execution. It is reset after the initialization.
  std::shared_ptr<const core::MergeJoinNode> joinNode_;

  std::vector<column_index_t> leftKeyChannels_;
  std::vector<column_index_t> rightKeyChannels_;
  std::vector<IdentityProjection> leftProjections_;
  std::vector<IdentityProjection> rightProjections_;

  // Join filter.
  std::unique_ptr<ExprSet> filter_;

  // Join filter input type.
  RowTypePtr filterInputType_;

  // Maps 'filterInputType_' channels to the corresponding channels in output_,
  // if any.
  folly::F14FastMap<column_index_t, column_index_t> filterInputToOutputChannel_;

  // Maps left-side input channels to channels in 'filterInputType_', excluding
  // those in 'filterInputToOutputChannel_'.
  std::vector<IdentityProjection> filterLeftInputProjections_;

  // Maps right-side input channels to channels in 'filterInputType_', excluding
  // those in 'filterInputToOutputChannel_'.
  std::vector<IdentityProjection> filterRightInputProjections_;

  // Reusable memory for filter evaluation.
  RowVectorPtr filterInput_;
  SelectivityVector filterRows_;
  std::vector<VectorPtr> filterResult_;
  DecodedVector decodedFilterResult_;

  // An instance of MergeJoinSource to pull batches of right side input from.
  std::shared_ptr<MergeJoinSource> rightSource_;

  // Latest batch of input from the right side.
  RowVectorPtr rightInput_;

  // Row number on the left side (input_) to process next.
  vector_size_t leftRowIndex_{0};

  // Row number on the right side (rightInput_) to process next.
  vector_size_t rightRowIndex_{0};

  // A set of rows with matching keys on the left side.
  std::optional<Match> leftMatch_;

  // A set of rows with matching keys on the right side.
  std::optional<Match> rightMatch_;

  RowVectorPtr output_;

  // Number of rows accumulated in the output_.
  vector_size_t outputSize_;

  // A future that will be completed when right side input becomes available.
  ContinueFuture futureRightSideInput_{ContinueFuture::makeEmpty()};

  // True if all the right side data has been received.
  bool noMoreRightInput_{false};

  bool leftHasDrained_{false};
  bool rightHasDrained_{false};

  bool leftJoinForFullFinished_{false};
  bool rightJoinForFullFinished_{false};
  std::optional<Match> leftForRightJoinMatch_;
  std::optional<Match> rightForRightJoinMatch_;
};
} // namespace facebook::velox::exec
