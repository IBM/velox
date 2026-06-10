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

#include "velox/expression/ExprEvaluatorV2.h"

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::exec {

// Pipeline implementation lands incrementally across steps 4-10.  For
// now every entry point is a no-op stub that fails loudly if reached.

void ExprEvaluatorV2::evaluate(
    EvalFrame& /*frame*/,
    const ExprSetV2* /*parentSet*/) {
  VELOX_NYI("ExprEvaluatorV2::evaluate lands in step 4 of the refactor.");
}

void ExprEvaluatorV2::evaluateFrame(
    EvalFrame& /*f*/,
    const ExprSetV2* /*parentSet*/) {
  VELOX_NYI();
}

void ExprEvaluatorV2::evaluateWithFieldPeeling(EvalFrame& /*f*/) {
  VELOX_NYI("Field peeling lands in step 5.");
}

void ExprEvaluatorV2::evaluateWithNullPruning(EvalFrame& /*f*/) {
  VELOX_NYI("Null pruning lands in step 7.");
}

void ExprEvaluatorV2::evaluateWithSharedSubexpr(EvalFrame& /*f*/) {
  VELOX_NYI("Shared-subexpr cache lands in step 8.");
}

void ExprEvaluatorV2::evaluateNodeBody(EvalFrame& /*f*/) {
  VELOX_NYI();
}

void ExprEvaluatorV2::evaluateFunctionCall(EvalFrame& /*f*/) {
  VELOX_NYI("Function-call leaf lands in step 4 / 9 / 10.");
}

void ExprEvaluatorV2::evaluateSpecialForm(EvalFrame& /*f*/) {
  VELOX_NYI("Special-form delegation lands in step 4.");
}

void ExprEvaluatorV2::applyFunction(EvalFrame& /*f*/) {
  VELOX_NYI();
}

void ExprEvaluatorV2::emitEmpty(EvalFrame& /*f*/) {
  VELOX_NYI();
}

} // namespace facebook::velox::exec
