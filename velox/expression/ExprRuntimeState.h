/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * Copyright (c) 2026 IBM Corporation.
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

#include <map>
#include <memory>
#include <vector>

#include <folly/container/F14Map.h>

#include "velox/common/time/CpuWallTimer.h"
#include "velox/expression/ExprStats.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::exec {

class ExprV2;

/// Identity of the input vectors a shared sub-expression was computed
/// over.  Used as a key in the per-Expr SharedSubexprCacheState.
/// Mirrors Expr::InputForSharedResults (Expr.h:738).
class InputForSharedResults {
 public:
  void addInput(const std::shared_ptr<BaseVector>& input) {
    inputVectors_.push_back(input.get());
    inputWeakVectors_.push_back(input);
  }

  bool operator<(const InputForSharedResults& other) const {
    return inputVectors_ < other.inputVectors_;
  }

  // True if any captured input has been freed since the entry was
  // inserted.  Stale entries are evicted on lookup.
  bool isExpired() const {
    for (const auto& input : inputWeakVectors_) {
      if (input.expired()) {
        return true;
      }
    }
    return false;
  }

 private:
  // Raw pointers used for ordering only; lifetime checked via the
  // parallel weak_ptr vector.
  std::vector<const BaseVector*> inputVectors_;
  std::vector<std::weak_ptr<BaseVector>> inputWeakVectors_;
};

/// Cached output for a previously-seen set of input identities.
/// Mirrors Expr::SharedResults (Expr.h:765).
struct SharedResults {
  // Rows for which 'sharedSubexprValues' has a valid value.
  std::unique_ptr<SelectivityVector> sharedSubexprRows;
  // The cached output, indexed alongside sharedSubexprRows.
  VectorPtr sharedSubexprValues;
};

/// Shared sub-expression result cache, indexed by the identity of the
/// captured input vectors.  Populated by the SharedSubexprCache phase.
struct SharedSubexprCacheState {
  std::map<InputForSharedResults, SharedResults> entries;
};

/// Per-Expr dictionary memoization state.  Mirrors the cluster of
/// member variables on Expr (baseOfDictionary*, dictionaryCache_,
/// cachedDictionaryIndices_, baseOfDictionaryRepeats_).  Populated by
/// the DictionaryMemo phase only on the peeled+mayCache path.
struct DictionaryMemoState {
  // Weak/raw pointers to the last cached base vector.  The weak_ptr is
  // checked for expiration to invalidate when inputs die between
  // batches; the raw pointer is the fast-path identity check.
  std::weak_ptr<BaseVector> baseOfDictionaryWeakPtr;
  BaseVector* baseOfDictionaryRawPtr{nullptr};

  // Strong reference taken on the second observation of the same base
  // (i.e. once baseOfDictionaryRepeats >= 1).  Pinning the base ensures
  // the dictionary indices remain valid.
  VectorPtr baseOfDictionary;

  // Number of consecutive batches that have used the same base.  0 on
  // first observation; caching begins on the transition 0 -> 1.
  int baseOfDictionaryRepeats{0};

  // Cached output values, indexed in the base dictionary's row space.
  VectorPtr dictionaryCache;

  // Set of rows in 'dictionaryCache' that are valid (have been computed
  // successfully).
  std::unique_ptr<SelectivityVector> cachedDictionaryIndices;
};

/// Per-Expr adaptive CPU sampling state machine.  Mirrors the cluster
/// of fields on Expr (Expr.h:802-828).  Decides at runtime whether to
/// pay CpuWallTimer overhead on each batch, based on a short
/// calibration phase that measures function cost vs. timer overhead.
struct AdaptiveSamplingState {
  /// Number of calibration batches.  Higher = less noise, slower to
  /// reach steady state.
  static constexpr uint32_t kCalibrationBatches = 5;

  enum class Phase : uint8_t {
    /// First batch: warm caches, discard timing.
    kWarmup,
    /// Next kCalibrationBatches batches: measure cost without timer.
    kCalibrating,
    /// Calibration done: timer overhead acceptable, always track.
    kAlwaysTrack,
    /// Calibration done: timer overhead too high, sample 1-of-N.
    kSampling,
  };

  Phase phase{Phase::kWarmup};

  // Used only during kCalibrating to measure function cost without
  // paying the CpuWallTimer overhead.
  std::optional<DeltaCpuWallTimeStopWatch> calibrationStopWatch;

  // Accumulated function wall time across calibration batches.
  uint64_t calibrationFunctionWallNanos{0};

  // Calibration batches seen so far.
  uint32_t calibrationBatchCount{0};

  // Final sampling rate decided after calibration (0 = always track).
  uint32_t samplingRate{0};

  // Counter for sampling cadence in kSampling phase.
  uint32_t samplingCounter{0};
};

/// Mutable per-Expr state that survives across evaluations.  One instance
/// per ExprV2 per ExprSetV2.  Not thread-safe: concurrent evaluations of
/// the same ExprSetV2 either need separate ExprRuntimeStateTrees or must
/// guard a shared one with a mutex.  Decision deferred until M3.
struct ExprRuntimeState {
  SharedSubexprCacheState sharedCache;
  DictionaryMemoState dictMemo;
  ExprStats stats;
  AdaptiveSamplingState adaptiveState;
  // Tracer is read directly from the source Expr via
  // ExprV2::sourceExpr()->outputTracer().  V2 doesn't own its own
  // tracer; ExprSet::maybeSetupTracers (inherited by ExprSetV2)
  // installs tracers on the V1 Expr nodes that V2 wraps.
};

/// Tree of runtime state parallel-indexed with an ExprV2 forest.  Look
/// up runtime state for a node by pointer.  Backed by a flat vector
/// for cache friendliness; lookups go through indexByNode_.
///
/// Built from the roots of an ExprSetV2.  Nodes shared between roots
/// (e.g. CSE subtrees) appear once in the tree and share state.
class ExprRuntimeStateTree {
 public:
  /// Builds a runtime-state tree covering 'roots' and all their
  /// reachable descendants.  Each unique ExprV2 node gets exactly one
  /// ExprRuntimeState.
  explicit ExprRuntimeStateTree(
      const std::vector<std::shared_ptr<ExprV2>>& roots);

  /// Returns the runtime state for 'node'.  'node' must be reachable
  /// from one of the roots this tree was built from.
  ExprRuntimeState& at(const ExprV2& node);

  /// Number of nodes covered by this tree.
  size_t size() const {
    return states_.size();
  }

 private:
  std::vector<ExprRuntimeState> states_;
  folly::F14FastMap<const ExprV2*, size_t> indexByNode_;
};

} // namespace facebook::velox::exec
