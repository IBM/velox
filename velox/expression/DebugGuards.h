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

#include "velox/common/base/Exceptions.h"
#include "velox/expression/EvalFrame.h"

namespace facebook::velox::exec {

#ifndef NDEBUG

/// Verifies row-space invariants across one phase scope:
///   - remainingRows is a subset of originalRows on exit.
///   - remainingRows.countSelected() never increased during the scope.
/// Install at the top of any pipeline function that may shrink (or
/// recurse with) remainingRows.
class DebugRemainingRowsGuard {
 public:
  explicit DebugRemainingRowsGuard(const EvalFrame& frame)
      : frame_{frame},
        countOnEntry_{frame.remainingRows.rows().countSelected()} {}

  ~DebugRemainingRowsGuard() {
    VELOX_DCHECK(frame_.remainingRows.rows().isSubset(frame_.originalRows));
    VELOX_DCHECK_LE(
        frame_.remainingRows.rows().countSelected(), countOnEntry_);
  }

 private:
  const EvalFrame& frame_;
  vector_size_t countOnEntry_;
};

/// Verifies frame-wide invariants across one top-level evaluate() call:
///   - inputValues empty on entry and exit (releaseGuard fired).
///   - result type matches expr type.
///   - result vector sized to at least originalRows.end().
/// Install once at the top of ExprEvaluatorV2::evaluateFrame().
class DebugEvaluateGuard {
 public:
  explicit DebugEvaluateGuard(const EvalFrame& frame) : frame_{frame} {
    VELOX_DCHECK(frame.inputValues.empty());
  }

  ~DebugEvaluateGuard() {
    VELOX_DCHECK(frame_.inputValues.empty());
    if (frame_.result != nullptr) {
      VELOX_DCHECK(
          frame_.result->type()->equivalent(*frame_.expr.type()));
      VELOX_DCHECK_GE(
          static_cast<vector_size_t>(frame_.result->size()),
          frame_.originalRows.end());
    }
  }

 private:
  const EvalFrame& frame_;
};

#else

class DebugRemainingRowsGuard {
 public:
  explicit DebugRemainingRowsGuard(const EvalFrame&) {}
};

class DebugEvaluateGuard {
 public:
  explicit DebugEvaluateGuard(const EvalFrame&) {}
};

#endif // NDEBUG

} // namespace facebook::velox::exec
