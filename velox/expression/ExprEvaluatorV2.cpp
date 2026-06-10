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

#include "velox/expression/ExprEvaluatorV2.h"

#include <folly/ScopeGuard.h>

#include "velox/common/base/Exceptions.h"
#include "velox/expression/DebugGuards.h"
#include "velox/expression/FieldReference.h"
#include "velox/expression/PeeledEncoding.h"
#include "velox/vector/LazyVector.h"

namespace facebook::velox::exec {

namespace {

void emitNullConstant(
    const TypePtr& type,
    memory::MemoryPool* pool,
    VectorPtr& result) {
  if (result == nullptr) {
    result = BaseVector::createNullConstant(type, 0, pool);
  }
}

// Mirrors the file-private isFlat() in Expr.cpp (line 355).
bool isFlat(const BaseVector& vector) {
  auto encoding = vector.encoding();
  if (encoding == VectorEncoding::Simple::LAZY) {
    if (!vector.asUnchecked<LazyVector>()->isLoaded()) {
      return true;
    }
    encoding = vector.loadedVector()->encoding();
  }
  return !(
      encoding == VectorEncoding::Simple::DICTIONARY ||
      encoding == VectorEncoding::Simple::CONSTANT);
}

// Result of attempting to peel the input field encodings.  Mirrors
// Expr::PeelEncodingsResult (Expr.cpp:1025).
struct FieldPeelResult {
  // Inner-row selection in the peeled row space.  nullptr if peel failed.
  SelectivityVector* newRows{nullptr};
  // True if the peel result is cacheable by base dictionary.  Triggers
  // the DictionaryMemo phase in step 6.
  bool mayCache{false};
};

// Peels input field encodings for the whole subtree rooted at expr.
// Mirrors Expr::peelEncodings (Expr.cpp:1025).  On success, mutates
// 'context' to install the peeled encoding and peeled vectors via
// 'saver'.
FieldPeelResult tryPeelFields(
    const ExprV2& expr,
    EvalCtx& context,
    ContextSaver& saver,
    const SelectivityVector& rows,
    LocalDecodedVector& localDecoded,
    LocalSelectivityVector& newRowsHolder,
    LocalSelectivityVector& finalRowsHolder) {
  if (context.wrapEncoding() == VectorEncoding::Simple::CONSTANT) {
    return {};
  }

  // Use finalSelection to generate peel to ensure those rows can be
  // translated and to ensure consistent peeling across multiple calls
  // for a shared sub-expression.
  const auto& rowsToPeel =
      context.isFinalSelection() ? rows : *context.finalSelection();

  std::vector<VectorPtr> vectorsToPeel;
  vectorsToPeel.reserve(expr.distinctFields().size());
  for (auto* field : expr.distinctFields()) {
    auto fieldIndex = field->index(context);
    auto fieldVector = context.getField(fieldIndex);
    if (fieldVector->isConstantEncoding()) {
      fieldVector = context.ensureFieldLoaded(fieldIndex, rowsToPeel);
    }
    vectorsToPeel.push_back(fieldVector);
  }

  VELOX_CHECK(!vectorsToPeel.empty());
  std::vector<VectorPtr> peeledVectors;
  auto peeledEncoding = PeeledEncoding::peel(
      vectorsToPeel,
      rowsToPeel,
      localDecoded,
      expr.propagatesNulls(),
      peeledVectors);
  if (!peeledEncoding) {
    return {};
  }

  SelectivityVector* newFinalSelection = nullptr;
  if (!context.isFinalSelection()) {
    newFinalSelection = peeledEncoding->translateToInnerRows(
        *context.finalSelection(), finalRowsHolder);
  }
  auto* newRows = peeledEncoding->translateToInnerRows(rows, newRowsHolder);

  context.saveAndReset(saver, rows);
  context.setPeeledEncoding(peeledEncoding);
  if (newFinalSelection) {
    *context.mutableFinalSelection() = newFinalSelection;
  }
  VELOX_DCHECK_EQ(peeledVectors.size(), expr.distinctFields().size());
  for (size_t i = 0; i < peeledVectors.size(); ++i) {
    auto fieldIndex = expr.distinctFields()[i]->index(context);
    context.setPeeled(fieldIndex, peeledVectors[i]);
  }

  bool mayCache = false;
  if (context.dictionaryMemoizationEnabled()) {
    mayCache = expr.distinctFields().size() == 1 &&
        VectorEncoding::isDictionary(context.wrapEncoding()) &&
        !peeledVectors[0]->memoDisabled();
  }
  return {newRows, mayCache};
}

} // namespace

void ExprEvaluatorV2::evaluate(EvalFrame& frame, const ExprSetV2* parentSet) {
  evaluateFrame(frame, parentSet);
}

void ExprEvaluatorV2::evaluateFrame(
    EvalFrame& f,
    const ExprSetV2* /*parentSet*/) {
  DebugEvaluateGuard outerGuard{f};

  // Fast path: delegate to V1's evalFlatNoNulls.  Gated identically to
  // V1's eval() (see Expr.cpp:821).  Bit-identical to V1 by
  // construction.
  if (f.supportsFlatNoNullsFastPath && f.ctx.throwOnError() &&
      f.ctx.inputFlatNoNulls() &&
      f.ctx.execCtx()->queryCtx()->queryConfig().exprEvalFlatNoNulls()) {
    f.expr.sourceExpr()->evalFlatNoNulls(
        f.originalRows, f.ctx, f.result, /*parentExprSet=*/nullptr);
    return;
  }

  // TODO(step 10): install ExprExceptionContext / ExceptionContextSetter
  // here.  Skipped during step 4 because exception-context wiring
  // requires the V2 equivalent of Expr::onException, which lands later.

  if (!f.originalRows.hasSelections()) {
    emitEmpty(f);
    return;
  }

  // TODO(step 5+): port the lazy-loading decision tree from
  // Expr::eval() (lines 868-887).  For step 4 the A/B harness only
  // covers expressions with no lazy inputs, so lazy loading is a no-op.

  if (f.expr.inputs().empty()) {
    evaluateNodeBody(f);
    return;
  }

  evaluateWithFieldPeeling(f);
}

void ExprEvaluatorV2::evaluateWithFieldPeeling(EvalFrame& f) {
  DebugRemainingRowsGuard guard{f};

  // Mirrors Expr::evalEncodings (Expr.cpp:1101).  Gating identical to
  // V1: must be deterministic, must not skip field-dependent
  // optimizations, peeling must be enabled, and no input field is
  // already flat (in which case peeling can't help).
  if (!f.deterministic || f.skipFieldDependentOptimizations ||
      !f.ctx.peelingEnabled(f.remainingRows.rows())) {
    evaluateWithNullPruning(f);
    return;
  }
  for (auto* field : f.expr.distinctFields()) {
    if (isFlat(*f.ctx.getField(field->index(f.ctx)))) {
      evaluateWithNullPruning(f);
      return;
    }
  }

  VectorPtr wrappedResult;
  withContextSaver([&](ContextSaver& saver) {
    LocalSelectivityVector newRowsHolder(f.ctx);
    LocalSelectivityVector finalRowsHolder(f.ctx);
    LocalDecodedVector decodedHolder(f.ctx);

    auto peel = tryPeelFields(
        f.expr,
        f.ctx,
        saver,
        f.remainingRows.rows(),
        decodedHolder,
        newRowsHolder,
        finalRowsHolder);
    if (peel.newRows == nullptr) {
      return;
    }

    VectorPtr peeledResult;
    if (peel.newRows->hasSelections()) {
      // Construct an inner frame on the peeled inner row space and
      // skip past evaluateWithFieldPeeling -- we've already peeled.
      // TODO(step 6): when peel.mayCache, route through
      // DictionaryMemo::evaluate before reaching null pruning.
      EvalFrame innerFrame{
          f.expr, f.runtimeStates, f.ctx, *peel.newRows, peeledResult};
      evaluateWithNullPruning(innerFrame);
    }

    wrappedResult = f.ctx.getPeeledEncoding()->wrap(
        f.expr.type(), f.ctx.pool(), peeledResult, f.remainingRows.rows());
  });

  if (wrappedResult != nullptr) {
    f.ctx.moveOrCopyResult(wrappedResult, f.remainingRows.rows(), f.result);
    return;
  }
  // Peeling did not produce a result (e.g. no peel possible) -- fall
  // through to null pruning on the original row space.
  evaluateWithNullPruning(f);
}

void ExprEvaluatorV2::evaluateWithNullPruning(EvalFrame& f) {
  DebugRemainingRowsGuard guard{f};
  // TODO(step 7): port NullPruning::tryPrune from Expr::evalWithNulls.
  // For step 4 this layer is a pass-through.
  evaluateWithSharedSubexpr(f);
}

void ExprEvaluatorV2::evaluateWithSharedSubexpr(EvalFrame& f) {
  DebugRemainingRowsGuard guard{f};
  // TODO(step 8): port SharedSubexprCache::tryReuse from
  // Expr::evaluateSharedSubexpr.  For step 4 this layer is a
  // pass-through.
  evaluateNodeBody(f);
}

void ExprEvaluatorV2::evaluateNodeBody(EvalFrame& f) {
  if (f.isSpecialForm) {
    evaluateSpecialForm(f);
    return;
  }
  evaluateFunctionCall(f);
}

void ExprEvaluatorV2::evaluateFunctionCall(EvalFrame& f) {
  DebugRemainingRowsGuard guard{f};
  auto releaseGuard = folly::makeGuard([&]() {
    f.ctx.releaseVectors(f.inputValues);
    f.inputValues.clear();
  });

  // TODO(step 9): port ArgEval::evaluate (default-null vs
  // preserve-null strategies, error swapping).  For step 4 the
  // harness restricts to expressions with no-null inputs, where
  // preserve-null and default-null are equivalent.
  f.inputValues.resize(f.expr.inputs().size());
  for (size_t i = 0; i < f.expr.inputs().size(); ++i) {
    auto& childExpr = *f.expr.inputs()[i];
    EvalFrame childFrame{
        childExpr,
        f.runtimeStates,
        f.ctx,
        f.remainingRows.rows(),
        f.inputValues[i]};
    evaluate(childFrame, /*parentSet=*/nullptr);
  }

  // TODO(step 5): port ArgPeeling::tryApply.  For step 4 we always
  // apply on un-peeled args.
  applyFunction(f);

  // TODO(step 9): write nulls for any rows that ArgEval pruned.  For
  // step 4 ArgEval never prunes, so remainingRows.hasChanged() is
  // always false.
  VELOX_DCHECK(!f.remainingRows.hasChanged());
}

void ExprEvaluatorV2::evaluateSpecialForm(EvalFrame& f) {
  DebugRemainingRowsGuard guard{f};
  // Delegate to the V1 Expr's evalSpecialForm.  Migration of each
  // special form to a native V2 implementation happens in step 12.
  f.expr.sourceExpr()->evalSpecialForm(
      f.remainingRows.rows(), f.ctx, f.result);
}

void ExprEvaluatorV2::applyFunction(EvalFrame& f) {
  // TODO(step 10): port listener invocation, CPU timing, adaptive
  // sampling, tracer hooks.  Step 4 invokes the bare function.
  VELOX_CHECK_NOT_NULL(f.expr.vectorFunction());
  f.expr.vectorFunction()->apply(
      f.remainingRows.rows(),
      f.inputValues,
      f.expr.type(),
      f.ctx,
      f.result);
}

void ExprEvaluatorV2::emitEmpty(EvalFrame& f) {
  emitNullConstant(f.expr.type(), f.ctx.pool(), f.result);
}

} // namespace facebook::velox::exec
