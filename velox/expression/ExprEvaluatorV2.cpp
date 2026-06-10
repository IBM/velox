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
#include "velox/expression/ScopedVarSetter.h"
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

// Mirrors Expr.cpp:1438.
bool isPeelable(VectorEncoding::Simple encoding) {
  return encoding == VectorEncoding::Simple::CONSTANT ||
      encoding == VectorEncoding::Simple::DICTIONARY;
}

// Mirrors Expr::throwArgumentErrors (Expr.cpp:1472).
bool throwArgumentErrors(const EvalFrame& f) {
  return f.ctx.throwOnError() &&
      (!f.defaultNulls ||
       (f.supportsFlatNoNullsFastPath && f.ctx.inputFlatNoNulls()));
}

// Mirrors mergeOrThrowArgumentErrors (Expr.cpp:339).
void mergeOrThrowArgumentErrors(
    const SelectivityVector& rows,
    EvalErrorsPtr& originalErrors,
    EvalErrorsPtr& argumentErrors,
    EvalCtx& context) {
  if (argumentErrors) {
    if (context.throwOnError()) {
      argumentErrors->throwFirstError(rows);
    }
    context.addErrors(rows, argumentErrors, originalErrors);
  }
  context.swapErrors(originalErrors);
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
      EvalFrame innerFrame{
          f.expr, f.runtimeStates, f.ctx, *peel.newRows, peeledResult};
      if (peel.mayCache) {
        evaluateDictionaryMemo(innerFrame);
      } else {
        evaluateWithNullPruning(innerFrame);
      }
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

void ExprEvaluatorV2::evaluateDictionaryMemo(EvalFrame& f) {
  // Mirrors Expr::evalWithMemo (Expr.cpp:1246).  Reached only from
  // FieldPeeling when peel.mayCache is true.
  auto& memo = f.nodeRuntime.dictMemo;

  VectorPtr base;
  f.expr.distinctFields()[0]->evalSpecialForm(
      f.remainingRows.rows(), f.ctx, base);

  // Cache miss: the base vector identity has changed or the previous
  // weak reference expired.  Reset cache state and recompute.
  if (base.get() != memo.baseOfDictionaryRawPtr ||
      memo.baseOfDictionaryWeakPtr.expired()) {
    memo.baseOfDictionaryRepeats = 0;
    memo.baseOfDictionaryWeakPtr.reset();
    memo.baseOfDictionaryRawPtr = nullptr;
    f.ctx.releaseVector(memo.baseOfDictionary);
    f.ctx.releaseVector(memo.dictionaryCache);

    evaluateWithNullPruning(f);
    memo.baseOfDictionaryWeakPtr = base;
    memo.baseOfDictionaryRawPtr = base.get();
    return;
  }

  // First repeat of the same base: start caching.
  if (memo.baseOfDictionaryRepeats == 0) {
    evaluateWithNullPruning(f);
    ++memo.baseOfDictionaryRepeats;
    memo.baseOfDictionary = base;
    memo.dictionaryCache = f.result;
    if (!memo.cachedDictionaryIndices) {
      memo.cachedDictionaryIndices = f.ctx.execCtx()->getSelectivityVector(
          f.remainingRows.rows().end());
    }
    *memo.cachedDictionaryIndices = f.remainingRows.rows();
    f.ctx.deselectErrors(*memo.cachedDictionaryIndices);
    return;
  }

  ++memo.baseOfDictionaryRepeats;

  // Copy cached values into the result for cached rows.
  if (memo.cachedDictionaryIndices) {
    LocalSelectivityVector cachedHolder(f.ctx, f.remainingRows.rows());
    auto* cached = cachedHolder.get();
    VELOX_DCHECK(cached != nullptr);
    cached->intersect(*memo.cachedDictionaryIndices);
    if (cached->hasSelections()) {
      f.ctx.ensureWritable(
          f.remainingRows.rows(), f.expr.type(), f.result);
      f.result->copy(memo.dictionaryCache.get(), *cached, nullptr);
    }
  }

  // Compute uncached rows by recursing on a child frame.
  LocalSelectivityVector uncachedHolder(f.ctx, f.remainingRows.rows());
  auto* uncached = uncachedHolder.get();
  VELOX_DCHECK(uncached != nullptr);
  if (memo.cachedDictionaryIndices) {
    uncached->deselect(*memo.cachedDictionaryIndices);
  }

  if (uncached->hasSelections()) {
    // Fix finalSelection at the outer rows if uncached is a strict
    // subset, to avoid losing values not in uncached that were copied
    // earlier into 'result' from cached rows.
    ScopedFinalSelectionSetter finalSelectionSetter(
        f.ctx,
        &f.remainingRows.rows(),
        uncached->countSelected() < f.remainingRows.rows().countSelected());

    EvalFrame uncachedFrame{
        f.expr, f.runtimeStates, f.ctx, *uncached, f.result};
    evaluateWithNullPruning(uncachedFrame);
    f.ctx.deselectErrors(*uncached);

    if (uncached->hasSelections()) {
      // TODO(step 14): register with V2's memo invalidation registry so
      // ExprSet::clearMemo / clearCache also clears V2 state.  V1 calls
      // context.exprSet()->addToMemo(this) here; equivalent V2 plumbing
      // is deferred to the cutover step.
      auto newCacheSize = uncached->end();

      LocalSelectivityVector allUncached(f.ctx, memo.dictionaryCache->size());
      allUncached.get()->setAll();
      allUncached.get()->deselect(*memo.cachedDictionaryIndices);
      f.ctx.ensureWritable(
          *allUncached.get(), f.expr.type(), memo.dictionaryCache);

      if (memo.cachedDictionaryIndices->size() < newCacheSize) {
        memo.cachedDictionaryIndices->resize(newCacheSize, false);
      }
      memo.cachedDictionaryIndices->select(*uncached);

      if (memo.dictionaryCache->size() < uncached->end()) {
        memo.dictionaryCache->resize(uncached->end());
      }
      memo.dictionaryCache->copy(f.result.get(), *uncached, nullptr);
    }
  }
  f.ctx.releaseVector(base);
}

namespace {

// Mirrors Expr::removeSureNulls (Expr.cpp:1157).  Computes the
// non-null subset of 'rows' across all distinct input fields.  Returns
// true if any sure-null row was removed; in that case 'nullHolder'
// owns the non-null SelectivityVector.
bool removeSureNulls(
    const ExprV2& expr,
    EvalCtx& context,
    const SelectivityVector& rows,
    LocalSelectivityVector& nullHolder) {
  SelectivityVector* result = nullptr;
  for (auto* field : expr.distinctFields()) {
    VectorPtr values;
    field->evalSpecialForm(rows, context, values);

    if (isLazyNotLoaded(*values)) {
      continue;
    }

    if (values->mayHaveNulls()) {
      LocalDecodedVector decoded(context, *values, rows);
      if (auto* rawNulls = decoded->nulls(&rows)) {
        if (!result) {
          result = nullHolder.get(rows);
        }
        auto* bits = result->asMutableRange().bits();
        bits::andBits(bits, rawNulls, rows.begin(), rows.end());
      }
    }
  }
  if (result == nullptr) {
    return false;
  }
  result->updateBounds();
  return result->countSelected() < rows.countSelected();
}

} // namespace

void ExprEvaluatorV2::evaluateWithNullPruning(EvalFrame& f) {
  DebugRemainingRowsGuard guard{f};

  // Mirrors Expr::evalWithNulls (Expr.cpp:1201).  Only attempts null
  // pruning when the expression propagates nulls and field-dependent
  // optimizations are not skipped.
  if (!f.propagatesNulls || f.skipFieldDependentOptimizations) {
    evaluateWithSharedSubexpr(f);
    return;
  }

  // Quick reject: if no distinct field may have nulls, skip the
  // expensive removeSureNulls work.
  bool mayHaveNulls = false;
  for (auto* field : f.expr.distinctFields()) {
    const auto& vector = f.ctx.getField(field->index(f.ctx));
    if (isLazyNotLoaded(*vector)) {
      continue;
    }
    if (vector->mayHaveNulls()) {
      mayHaveNulls = true;
      break;
    }
  }
  if (!mayHaveNulls) {
    evaluateWithSharedSubexpr(f);
    return;
  }

  LocalSelectivityVector nonNullHolder(f.ctx);
  if (!removeSureNulls(
          f.expr, f.ctx, f.remainingRows.rows(), nonNullHolder)) {
    evaluateWithSharedSubexpr(f);
    return;
  }

  // Default-null pruning fired.  Recurse on the non-null subset under
  // a scoped nullsPruned flag, then null-fill the rows we removed.
  ScopedVarSetter noMoreNulls(f.ctx.mutableNullsPruned(), true);
  auto* nonNullRows = nonNullHolder.get();

  if (nonNullRows->hasSelections()) {
    EvalFrame innerFrame{
        f.expr, f.runtimeStates, f.ctx, *nonNullRows, f.result};
    evaluateWithSharedSubexpr(innerFrame);
  }

  // addNulls writes nulls into 'result' for rows in originalRows but
  // not in nonNullRows.
  EvalCtx::addNulls(
      f.remainingRows.rows(),
      nonNullRows->asRange().bits(),
      f.ctx,
      f.expr.type(),
      f.result);
}

void ExprEvaluatorV2::evaluateWithSharedSubexpr(EvalFrame& f) {
  DebugRemainingRowsGuard guard{f};

  // Mirrors the wrapper in Expr::evalAll (Expr.cpp:1459): only invoke
  // the shared-subexpr cache when the expression is a CSE candidate.
  if (!f.deterministic || !f.expr.isMultiplyReferenced() ||
      f.expr.inputs().empty() || !f.ctx.sharedSubExpressionReuseEnabled()) {
    evaluateNodeBody(f);
    return;
  }

  // Mirrors Expr::evaluateSharedSubexpr (Expr.cpp:899).  Cache keyed
  // by the identity of all distinct input field vectors.
  auto& cache = f.nodeRuntime.sharedCache.entries;

  InputForSharedResults key;
  for (auto* field : f.expr.distinctFields()) {
    key.addInput(f.ctx.getField(field->index(f.ctx)));
  }

  auto it = cache.find(key);
  if (it != cache.end() && it->first.isExpired()) {
    cache.erase(it);
    it = cache.end();
  }

  if (it == cache.end()) {
    auto max = f.ctx.maxSharedSubexprResultsCached();
    if (cache.size() < max) {
      it = cache.insert({std::move(key), SharedResults{}}).first;
    } else {
      // Cache full: evaluate without caching.
      evaluateNodeBody(f);
      return;
    }
  }

  auto& sharedRows = it->second.sharedSubexprRows;
  auto& sharedValues = it->second.sharedSubexprValues;

  // First observation under this key: compute, store, return.
  if (sharedValues == nullptr) {
    evaluateNodeBody(f);
    if (!sharedRows) {
      sharedRows = f.ctx.execCtx()->getSelectivityVector(
          f.remainingRows.rows().end());
    }
    *sharedRows = f.remainingRows.rows();
    if (f.ctx.errors()) {
      f.ctx.deselectErrors(*sharedRows);
      if (!sharedRows->hasSelections()) {
        // No usable rows; don't cache.
        return;
      }
    }
    sharedValues = f.result;
    return;
  }

  // Full hit: every requested row is already cached.
  if (f.remainingRows.rows().isSubset(*sharedRows)) {
    f.ctx.moveOrCopyResult(sharedValues, f.remainingRows.rows(), f.result);
    return;
  }

  // Partial hit: compute the missing rows on top of cached results.
  LocalSelectivityVector missingHolder(f.ctx, f.remainingRows.rows());
  auto* missing = missingHolder.get();
  missing->deselect(*sharedRows);
  VELOX_DCHECK(missing->hasSelections());

  // Final selection must cover sharedRows ∪ missing ∪ existing
  // finalSelection so values outside 'missing' aren't lost when the
  // child writes into sharedValues.
  LocalSelectivityVector newFinalSelHolder(f.ctx, *sharedRows);
  auto* newFinalSel = newFinalSelHolder.get();
  newFinalSel->select(*missing);
  if (!f.ctx.isFinalSelection()) {
    newFinalSel->select(*f.ctx.finalSelection());
  }
  ScopedFinalSelectionSetter finalSetter(
      f.ctx,
      newFinalSel,
      /*checkCondition=*/true,
      /*override=*/true);

  EvalFrame missingFrame{
      f.expr, f.runtimeStates, f.ctx, *missing, sharedValues};
  evaluateNodeBody(missingFrame);

  f.ctx.deselectErrors(*missing);
  sharedRows->select(*missing);

  if (f.ctx.errors()) {
    LocalSelectivityVector rowsWithoutErrorsHolder(
        f.ctx, f.remainingRows.rows());
    auto* rowsWithoutErrors = rowsWithoutErrorsHolder.get();
    f.ctx.deselectErrors(*rowsWithoutErrors);
    f.ctx.moveOrCopyResult(sharedValues, *rowsWithoutErrors, f.result);
  } else {
    f.ctx.moveOrCopyResult(sharedValues, f.remainingRows.rows(), f.result);
  }
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

  // Reset before each arg-eval attempt; the strategies populate
  // inputValues, and ArgPeeling reads tryPeelArgs.
  f.tryPeelArgs = f.deterministic;

  bool argsOk = f.defaultNulls ? evalArgsDefaultNull(f) : evalArgsPreserveNull(f);
  if (!argsOk) {
    // setAllNulls already applied to originalRows.
    return;
  }

  if (!f.tryPeelArgs || !tryApplyWithPeeling(f)) {
    applyFunction(f);
  }

  // Write nulls for any rows that ArgEval pruned (default-null path).
  if (f.remainingRows.hasChanged()) {
    EvalCtx::addNulls(
        f.originalRows,
        f.remainingRows.rows().asRange().bits(),
        f.ctx,
        f.expr.type(),
        f.result);
  }
}

bool ExprEvaluatorV2::evalArgsDefaultNull(EvalFrame& f) {
  // Mirrors Expr::evalArgsDefaultNulls (Expr.cpp:380).  For each child:
  // evaluate, then deselect rows that are null and have no error.
  EvalErrorsPtr argumentErrors;
  EvalErrorsPtr originalErrors;
  LocalDecodedVector decoded(f.ctx);

  // Set aside pre-existing errors so we can distinguish argument errors.
  if (f.ctx.errors()) {
    f.ctx.swapErrors(originalErrors);
  }

  f.inputValues.resize(f.expr.inputs().size());
  {
    ScopedVarSetter throwErrors(
        f.ctx.mutableThrowOnError(), throwArgumentErrors(f));

    for (size_t i = 0; i < f.expr.inputs().size(); ++i) {
      auto& childExpr = *f.expr.inputs()[i];
      EvalFrame childFrame{
          childExpr,
          f.runtimeStates,
          f.ctx,
          f.remainingRows.rows(),
          f.inputValues[i]};
      evaluate(childFrame, /*parentSet=*/nullptr);
      f.tryPeelArgs =
          f.tryPeelArgs && isPeelable(f.inputValues[i]->encoding());

      const uint64_t* flatNulls = nullptr;
      auto& arg = f.inputValues[i];
      if (arg->mayHaveNulls()) {
        decoded.get()->decode(*arg, f.remainingRows.rows());
        flatNulls = decoded.get()->nulls(&f.remainingRows.rows());
      }

      if (f.ctx.errors()) {
        f.ctx.ensureErrorsVectorSize(f.remainingRows.rows().end());
        auto* newErrors = f.ctx.errors();
        if (flatNulls) {
          // Null without error removes the row; null with error keeps
          // the error.
          auto errorNulls = newErrors->errorFlags();
          auto* rowBits = f.remainingRows.mutableRows().asMutableRange().bits();
          auto nwords = bits::nwords(f.remainingRows.rows().end());
          for (size_t w = 0; w < nwords; ++w) {
            auto nullNoError =
                errorNulls ? flatNulls[w] | errorNulls[w] : flatNulls[w];
            rowBits[w] &= nullNoError;
          }
          f.remainingRows.mutableRows().updateBounds();
        }
        f.ctx.moveAppendErrors(argumentErrors);
      } else if (flatNulls) {
        f.remainingRows.deselectNulls(flatNulls);
      }

      if (!f.remainingRows.rows().hasSelections()) {
        break;
      }
    }
  }

  mergeOrThrowArgumentErrors(
      f.remainingRows.rows(), originalErrors, argumentErrors, f.ctx);

  if (!f.remainingRows.deselectErrors()) {
    f.ctx.releaseVectors(f.inputValues);
    f.inputValues.clear();
    setAllNulls(f, f.remainingRows.originalRows());
    return false;
  }
  return true;
}

bool ExprEvaluatorV2::evalArgsPreserveNull(EvalFrame& f) {
  // Mirrors Expr::evalArgsWithNulls (Expr.cpp:455).  Nulls are not
  // pruned; only error rows are removed.
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
    f.tryPeelArgs =
        f.tryPeelArgs && isPeelable(f.inputValues[i]->encoding());

    if (!f.remainingRows.deselectErrors()) {
      break;
    }
  }
  if (!f.remainingRows.rows().hasSelections()) {
    f.ctx.releaseVectors(f.inputValues);
    f.inputValues.clear();
    setAllNulls(f, f.remainingRows.originalRows());
    return false;
  }
  return true;
}

bool ExprEvaluatorV2::tryApplyWithPeeling(EvalFrame& f) {
  // Mirrors Expr::applyFunctionWithPeeling (Expr.cpp:1534).
  const auto& applyRows = f.remainingRows.rows();
  LocalDecodedVector localDecoded(f.ctx);
  LocalSelectivityVector newRowsHolder(f.ctx);

  // Peeling-suppressed path (small batches): handle the
  // single-dictionary and single-constant cases explicitly to preserve
  // the "flat-or-constant inputs" guarantee that vector functions rely
  // on.
  if (!f.ctx.peelingEnabled(applyRows) &&
      !(f.inputValues.size() == 1 &&
        f.inputValues[0]->encoding() == VectorEncoding::Simple::CONSTANT)) {
    int dictIndex = -1;
    bool canFlatten = true;

    for (int i = 0; i < static_cast<int>(f.inputValues.size()); ++i) {
      auto encoding = f.inputValues[i]->encoding();
      if (encoding == VectorEncoding::Simple::DICTIONARY) {
        if (dictIndex != -1) {
          canFlatten = false;
          break;
        }
        dictIndex = i;
      }
    }
    if (canFlatten && dictIndex != -1) {
      BaseVector::flattenVector(f.inputValues[dictIndex]);
      applyFunction(f);
      return true;
    }
    return false;
  }

  // Attempt peeling on the evaluated input vectors.
  std::vector<VectorPtr> peeledVectors;
  auto peeledEncoding = PeeledEncoding::peel(
      f.inputValues,
      applyRows,
      localDecoded,
      f.expr.metadata().defaultNullBehavior,
      peeledVectors);
  if (!peeledEncoding) {
    return false;
  }
  f.inputValues = std::move(peeledVectors);
  peeledVectors.clear();

  auto* newRows = peeledEncoding->translateToInnerRows(applyRows, newRowsHolder);

  withContextSaver([&](ContextSaver& saver) {
    f.ctx.saveAndReset(saver, applyRows);
    f.ctx.setPeeledEncoding(peeledEncoding);

    VectorPtr peeledResult;
    f.expr.vectorFunction()->apply(
        *newRows, f.inputValues, f.expr.type(), f.ctx, peeledResult);

    VectorPtr wrappedResult = f.ctx.getPeeledEncoding()->wrap(
        f.expr.type(), f.ctx.pool(), peeledResult, applyRows);
    f.ctx.moveOrCopyResult(wrappedResult, applyRows, f.result);

    f.ctx.releaseVector(peeledResult);
  });

  return true;
}

void ExprEvaluatorV2::setAllNulls(
    EvalFrame& f,
    const SelectivityVector& rows) {
  // Mirrors Expr::setAllNulls (Expr.cpp:1353).
  if (f.result) {
    BaseVector::ensureWritable(rows, f.expr.type(), f.ctx.pool(), f.result);
    LocalSelectivityVector notNulls(f.ctx, rows.end());
    notNulls.get()->setAll();
    notNulls.get()->deselect(rows);
    f.result->addNulls(notNulls.get()->asRange().bits(), rows);
    return;
  }
  f.result =
      BaseVector::createNullConstant(f.expr.type(), rows.end(), f.ctx.pool());
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
