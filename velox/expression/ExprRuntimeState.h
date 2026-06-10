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

#include <memory>
#include <vector>

#include <folly/container/F14Map.h>

#include "velox/expression/ExprStats.h"

namespace facebook::velox::exec {

class ExprV2;

/// Placeholder for shared-subexpression cache state.  Populated in step 8.
struct SharedSubexprCacheState {};

/// Placeholder for dictionary memoization state.  Populated in step 6.
struct DictionaryMemoState {};

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

/// Tree of runtime state parallel-indexed with an ExprV2 tree.  Look up
/// runtime state for a node by pointer.  Implementation uses a flat
/// vector for cache friendliness; lookups go through indexByNode_.
class ExprRuntimeStateTree {
 public:
  /// Builds a runtime-state tree mirroring the shape of the ExprV2 tree
  /// rooted at 'root'.  Each node in the tree gets one ExprRuntimeState.
  explicit ExprRuntimeStateTree(const ExprV2& root);

  /// Returns the runtime state for 'node'.  'node' must be a member of
  /// the tree this object was built from.
  ExprRuntimeState& at(const ExprV2& node);

 private:
  std::vector<ExprRuntimeState> states_;
  folly::F14FastMap<const ExprV2*, size_t> indexByNode_;
};

} // namespace facebook::velox::exec
