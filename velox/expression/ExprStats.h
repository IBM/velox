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

#include <cstdint>

#include "velox/common/time/CpuWallTimer.h"

namespace facebook::velox::exec {

struct ExprStats {
  /// Requires QueryConfig.exprTrackCpuUsage() to be 'true'.
  CpuWallTiming timing;

  /// Number of processed rows.
  uint64_t numProcessedRows{0};

  /// Number of processed vectors / batches. Allows to compute average batch
  /// size.
  uint64_t numProcessedVectors{0};

  /// Whether default-null behavior of an expression resulted in skipping
  /// evaluation of rows.
  bool defaultNullRowsSkipped{false};

  /// Number of times Expr::evalWithMemo was entered. Less than
  /// numProcessedVectors when the peelEncodings cache-covers-base
  /// bypass fires (numMemoBypass counts those).
  uint64_t numEvalWithMemo{0};

  /// evalWithMemo path counters. Sum of these <= numEvalWithMemo;
  /// the remainder are calls that did no useful work (empty base
  /// after deselects, etc).
  ///
  /// numMemoBaseChange: the base of the input dictionary differs
  /// from the cached one, so the prior cache is dropped and this
  /// batch is evaluated from scratch.
  /// numMemoFirstRepeat: the second sighting of a base; populates
  /// dictionaryCache_ for the first time.
  /// numMemoEagerFill: subsequent batches over the same base where
  /// isCheapToReevaluate() let evalWithMemo speculatively fill
  /// uncached base positions in one shot. Hand back the cache.
  /// numMemoIncremental: subsequent batches that take the
  /// non-cheap incremental path (copy cached rows to a fresh
  /// result, evaluate uncached rows separately, extend the cache).
  uint64_t numMemoBaseChange{0};
  uint64_t numMemoFirstRepeat{0};
  uint64_t numMemoEagerFill{0};
  uint64_t numMemoIncremental{0};

  /// Times peelEncodings short-circuited because dictionaryCache_
  /// already covered every position of the peeled base. evalEncodings
  /// wraps the cache directly without calling evalWithMemo at all.
  /// This is the metric to watch for the cache-hit hot path.
  uint64_t numMemoBypass{0};

  /// Base positions evaluated by the eager-fill path that the caller
  /// did not request (the speculative work). Compare to
  /// numProcessedRows to gauge how much over-evaluation eager-fill
  /// performs in practice.
  uint64_t numEagerFillSpeculativeRows{0};

  /// Eager-fill chose to deselect already-cached positions before
  /// evaluating (numEagerFillDeselect) vs. re-evaluating the whole
  /// base without paying the deselect bitmap cost
  /// (numEagerFillFullReeval). Sums to numMemoEagerFill (modulo
  /// empty-toFill early-outs).
  uint64_t numEagerFillDeselect{0};
  uint64_t numEagerFillFullReeval{0};

  auto operator<=>(const ExprStats&) const = default;

  void add(const ExprStats& other) {
    timing.add(other.timing);
    numProcessedRows += other.numProcessedRows;
    numProcessedVectors += other.numProcessedVectors;
    defaultNullRowsSkipped |= other.defaultNullRowsSkipped;
    numEvalWithMemo += other.numEvalWithMemo;
    numMemoBaseChange += other.numMemoBaseChange;
    numMemoFirstRepeat += other.numMemoFirstRepeat;
    numMemoEagerFill += other.numMemoEagerFill;
    numMemoIncremental += other.numMemoIncremental;
    numMemoBypass += other.numMemoBypass;
    numEagerFillSpeculativeRows += other.numEagerFillSpeculativeRows;
    numEagerFillDeselect += other.numEagerFillDeselect;
    numEagerFillFullReeval += other.numEagerFillFullReeval;
  }

  std::string toString() const {
    return fmt::format(
        "timing: {}, numProcessedRows: {}, numProcessedVectors: {}, "
        "defaultNullRowsSkipped: {}, numEvalWithMemo: {}, "
        "numMemo(baseChange/firstRepeat/eagerFill/incremental/bypass): "
        "{}/{}/{}/{}/{}, numEagerFillSpeculativeRows: {}, "
        "numEagerFill(deselect/fullReeval): {}/{}",
        timing.toString(),
        numProcessedRows,
        numProcessedVectors,
        defaultNullRowsSkipped ? "true" : "false",
        numEvalWithMemo,
        numMemoBaseChange,
        numMemoFirstRepeat,
        numMemoEagerFill,
        numMemoIncremental,
        numMemoBypass,
        numEagerFillSpeculativeRows,
        numEagerFillDeselect,
        numEagerFillFullReeval);
  }
};
} // namespace facebook::velox::exec
