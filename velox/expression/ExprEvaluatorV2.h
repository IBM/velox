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

#include "velox/common/time/CpuWallTimer.h"
#include "velox/expression/EvalFrame.h"

namespace facebook::velox::exec {

class ExprSetV2;

/// Drives the staged evaluation pipeline against an EvalFrame.  Owns no
/// state; all per-call state lives on the frame, all cross-call state
/// lives in ExprRuntimeState.  Safe to share across threads.
///
/// Pipeline order matches V1 semantics exactly:
///
///   evaluateFrame                  entry guards
///     evaluateWithFieldPeeling     field peeling wrapper
///       evaluateWithNullPruning    null pruning wrapper
///         evaluateWithSharedSubexpr shared subexpr wrapper
///           evaluateNodeBody       special-form vs function-call fork
///             evaluateSpecialForm  (delegates to legacy Expr)
///             evaluateFunctionCall arg eval + arg peeling + apply
class ExprEvaluatorV2 {
 public:
  /// Public entry point.  Drives the pipeline against 'frame'.
  /// 'parentSet' is forwarded to the top-level exception scope; null
  /// for nested/non-top-level calls.
  void evaluate(EvalFrame& frame, const ExprSetV2* parentSet);

 private:
  // Pipeline phases.  Each name describes what wrapper or fork this
  // layer owns, not what the previous layer did.
  void evaluateFrame(EvalFrame& f, const ExprSetV2* parentSet);
  void evaluateWithFieldPeeling(EvalFrame& f);
  void evaluateDictionaryMemo(EvalFrame& f);
  void evaluateWithNullPruning(EvalFrame& f);
  void evaluateWithSharedSubexpr(EvalFrame& f);
  void evaluateNodeBody(EvalFrame& f);
  void evaluateFunctionCall(EvalFrame& f);
  void evaluateSpecialForm(EvalFrame& f);

  // Argument-evaluation strategies (Expr.cpp:380, 455).  Each populates
  // f.inputValues, may shrink f.remainingRows, and returns true if at
  // least one row survived (false means setAllNulls already applied).
  bool evalArgsDefaultNull(EvalFrame& f);
  bool evalArgsPreserveNull(EvalFrame& f);

  // Leaf operations.
  void applyFunction(EvalFrame& f);
  bool tryApplyWithPeeling(EvalFrame& f);
  void emitEmpty(EvalFrame& f);
  void setAllNulls(EvalFrame& f, const SelectivityVector& rows);

  // Returns a timer that updates f.nodeRuntime.stats.timing on
  // destruction, or nullptr if this batch should not be timed.
  // Mirrors Expr::cpuWallTimer (Expr.cpp:1619).
  std::unique_ptr<CpuWallTimer> cpuWallTimer(EvalFrame& f);

  // Advances the adaptive sampling state machine after the function
  // has been invoked.  Mirrors Expr::finalizeAdaptiveCalibration
  // (Expr.cpp:1650).
  void finalizeAdaptiveCalibration(
      EvalFrame& f,
      double maxOverheadPct,
      uint64_t timerOverheadNanos);
};

} // namespace facebook::velox::exec
