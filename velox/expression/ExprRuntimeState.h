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

#include <memory>
#include <vector>

#include <folly/container/F14Map.h>

#include "velox/expression/ExprStats.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::exec {

class ExprV2;

/// Placeholder for shared-subexpression cache state.  Populated in step 8.
struct SharedSubexprCacheState {};

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

/// Placeholder for adaptive CPU sampling state.  Populated in step 10.
struct AdaptiveSamplingState {};

/// Mutable per-Expr state that survives across evaluations.  One instance
/// per ExprV2 per ExprSetV2.  Not thread-safe: concurrent evaluations of
/// the same ExprSetV2 either need separate ExprRuntimeStateTrees or must
/// guard a shared one with a mutex.  Decision deferred until M3.
struct ExprRuntimeState {
  SharedSubexprCacheState sharedCache;
  DictionaryMemoState dictMemo;
  ExprStats stats;
  AdaptiveSamplingState adaptiveState;
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
