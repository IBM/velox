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

#include <vector>

#include "velox/expression/EvalCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/ExprRuntimeState.h"
#include "velox/expression/ExprV2.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::exec {

/// Per-call evaluation state for one ExprV2 node.  Lives on the stack of
/// the outermost ExprEvaluatorV2::evaluate() call.  Inner recursion
/// (peeling, null-pruning wrapper, shared-subexpr wrapper) constructs
/// fresh EvalFrames.
///
/// Row-space invariant:
///   - originalRows never changes after construction.
///   - remainingRows.rows() is always a subset of originalRows.
///   - Every phase operates on remainingRows.rows(), not originalRows.
///   - remainingRows shrinks monotonically as evaluation proceeds; it
///     never grows.
///   - originalRows is used only for result sizing, final null-fill,
///     and copying / wrapping back into the caller's row space.
struct EvalFrame {
  EvalFrame(
      ExprV2& exprIn,
      ExprRuntimeStateTree& runtimeStatesIn,
      EvalCtx& ctxIn,
      const SelectivityVector& rowsIn,
      VectorPtr& resultIn)
      : expr{exprIn},
        runtimeStates{runtimeStatesIn},
        nodeRuntime{runtimeStatesIn.at(exprIn)},
        ctx{ctxIn},
        originalRows{rowsIn},
        result{resultIn},
        remainingRows{rowsIn, ctxIn},
        tryPeelArgs{exprIn.deterministic()},
        defaultNulls{exprIn.metadata().defaultNullBehavior},
        propagatesNulls{exprIn.propagatesNulls()},
        deterministic{exprIn.deterministic()},
        isSpecialForm{exprIn.isSpecialForm()},
        supportsFlatNoNullsFastPath{exprIn.supportsFlatNoNullsFastPath()},
        hasConditionals{exprIn.hasConditionals()} {}

  // === Bindings (constant after construction) ===

  ExprV2& expr;
  // Tree-wide runtime state.  Used to look up runtime state for child
  // nodes when constructing inner frames.
  ExprRuntimeStateTree& runtimeStates;
  // Cached lookup: runtime state for 'expr'.  Avoids per-phase map
  // lookups via runtimeStates.
  ExprRuntimeState& nodeRuntime;
  EvalCtx& ctx;
  const SelectivityVector& originalRows;
  VectorPtr& result;

  // === Mutable state flowing between phases ===

  MutableRemainingRows remainingRows;

  // Evaluated child results, populated by ArgEval.  Moved off ExprV2;
  // lives on the frame so the same ExprV2 can be evaluated concurrently.
  std::vector<VectorPtr> inputValues;

  // Running conjunction tracked by ArgEval: are all evaluated child
  // encodings peelable?  Initialized from expr.deterministic(); cleared
  // when ArgEval sees a non-peelable encoding.
  bool tryPeelArgs;

  // === Compile-time flags cached once from expr ===

  bool defaultNulls;
  bool propagatesNulls;
  bool deterministic;
  bool isSpecialForm;
  bool supportsFlatNoNullsFastPath;
  bool hasConditionals;
};

} // namespace facebook::velox::exec
