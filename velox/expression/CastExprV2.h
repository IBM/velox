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

#include <functional>

#include "velox/common/base/Status.h"
// Reuse V1's CastOperator interface (defined in CastExpr.h) and
// CastHooks plumbing.  CastExprV2 is a parallel evaluation class but
// shares the same plug-in surface for custom-type cast registration.
#include "velox/expression/CastExpr.h"
#include "velox/expression/CastHooks.h"
#include "velox/expression/ExprConstants.h"
#include "velox/expression/FunctionCallToSpecialForm.h"
#include "velox/expression/SpecialForm.h"

namespace facebook::velox::exec {

class CastExprV2 : public SpecialForm {
 public:
  /// Builds a CastHooks instance for a CAST or TRY_CAST going through
  /// CastExprV2.  The factory is registered globally via
  /// setHooksFactory; V2 cast routing reads it during tree
  /// construction (ExprV2::from) rather than at compile time, which
  /// is how we keep V1's CastExpr.cpp untouched.
  using HooksFactory = std::function<
      std::shared_ptr<CastHooks>(const core::QueryConfig& config, bool isTryCast)>;

  /// @param type The target type of the cast expression
  /// @param expr The expression to cast
  /// @param trackCpuUsage Whether to track CPU usage
  CastExprV2(
      TypePtr type,
      ExprPtr&& expr,
      bool trackCpuUsage,
      bool isTryCast,
      std::shared_ptr<CastHooks> hooks)
      : SpecialForm(
            SpecialFormKind::kCast,
            type,
            std::vector<ExprPtr>({expr}),
            isTryCast ? expression::kTryCast : expression::kCast,
            false /* supportsFlatNoNullsFastPath */,
            trackCpuUsage),
        isTryCast_(isTryCast),
        hooks_(std::move(hooks)) {}

  /// Replaces the V2 cast hooks factory.  Default factory produces
  /// PrestoCastHooks; callers register their own factory at startup
  /// to plug in a vectorized variant (e.g., PrestoCastHooksV2) or
  /// any other CastHooks subclass.  Must be non-null.
  static void setHooksFactory(HooksFactory factory);

  /// Returns the currently-registered V2 cast hooks factory.  Never
  /// null.
  static const HooksFactory& hooksFactory();

  void evalSpecialForm(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override;

  std::string toString(bool recursive = true) const override;

  std::string toSql(std::vector<VectorPtr>*) const override;

  /// Casts 'input' from 'fromType' to 'toType' for selected 'rows'.
  void apply(
      const SelectivityVector& rows,
      const VectorPtr& input,
      exec::EvalCtx& context,
      const TypePtr& fromType,
      const TypePtr& toType,
      VectorPtr& result);

 private:
  VectorPtr castMap(
      const SelectivityVector& rows,
      const MapVector* input,
      exec::EvalCtx& context,
      const MapType& fromType,
      const MapType& toType);

  VectorPtr castArray(
      const SelectivityVector& rows,
      const ArrayVector* input,
      exec::EvalCtx& context,
      const ArrayType& fromType,
      const ArrayType& toType);

  VectorPtr castRow(
      const SelectivityVector& rows,
      const RowVector* input,
      exec::EvalCtx& context,
      const RowType& fromType,
      const TypePtr& toType);

  /// Apply the cast between decimal vectors.
  /// @param rows Non-null rows of the input vector.
  /// @param input The input decimal vector. It is guaranteed to be flat or
  /// constant.
  template <typename ToDecimalType>
  VectorPtr castDecimal(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& fromType,
      const TypePtr& toType);

  // Apply the cast to a vector after vector encodings being peeled off. The
  // input vector is guaranteed to be flat or constant.
  void castPeeled(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& fromType,
      const TypePtr& toType,
      VectorPtr& result);

  template <typename Func>
  void applyToSelectedNoThrowLocal(
      EvalCtx& context,
      const SelectivityVector& rows,
      VectorPtr& result,
      Func&& func);

  /// The per-row level Kernel
  /// @tparam ToKind The cast target type
  /// @tparam FromKind The expression type
  /// @tparam TPolicy The policy used by the cast
  /// @param row The index of the current row
  /// @param input The input vector (of type FromKind)
  /// @param result The output vector (of type ToKind)
  template <TypeKind ToKind, TypeKind FromKind, typename TPolicy>
  void castKernel(
      vector_size_t row,
      EvalCtx& context,
      const SimpleVector<typename TypeTraits<FromKind>::NativeType>* input,
      FlatVector<typename TypeTraits<ToKind>::NativeType>* result);

  VectorPtr castFromDate(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& toType);

  VectorPtr castToDate(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& fromType);

  VectorPtr castFromIntervalDayTime(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& toType);

  VectorPtr castFromTime(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& toType);

  VectorPtr castToTime(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& fromType);

  template <typename TInput, typename TOutput>
  void castDecimalToDecimalKernel(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& fromType,
      const TypePtr& toType,
      VectorPtr& castResult);

  template <typename TInput, typename TOutput>
  void castIntToDecimalKernel(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& toType,
      VectorPtr& castResult);

  template <typename TInput>
  VectorPtr castIntToBinary(
      const SelectivityVector& rows,
      exec::EvalCtx& context,
      const BaseVector& input);

  template <typename TInput, typename TOutput>
  void castFloatToDecimalKernel(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& toType,
      VectorPtr& castResult);

  template <typename T>
  void castVarcharToDecimalKernel(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& toType,
      VectorPtr& castResult);

  template <typename FromNativeType, TypeKind ToKind>
  VectorPtr castDecimalToFloat(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& fromType,
      const TypePtr& toType);

  template <typename FromNativeType, TypeKind ToKind>
  VectorPtr castDecimalToIntegral(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& fromType,
      const TypePtr& toType);

  template <typename FromNativeType>
  VectorPtr castDecimalToBoolean(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context);

  template <typename FromNativeType>
  VectorPtr castDecimalToPrimitive(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& fromType,
      const TypePtr& toType);

  template <TypeKind ToKind, TypeKind FromKind>
  void castPrimitives(
      const SelectivityVector& rows,
      exec::EvalCtx& context,
      const BaseVector& input,
      VectorPtr& result);

  template <typename FromNativeType>
  VectorPtr castDecimalToVarchar(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& fromType);

  template <TypeKind ToKind>
  void castPrimitivesDispatch(
      const TypePtr& fromType,
      const TypePtr& toType,
      const SelectivityVector& rows,
      exec::EvalCtx& context,
      const BaseVector& input,
      VectorPtr& result);

  VectorPtr castTimestampToVarchar(
      const TypePtr& toType,
      const SelectivityVector& rows,
      exec::EvalCtx& context,
      const BaseVector& input);

  // Casts basic numeric types to wider types.
  template <TypeKind ToKind, TypeKind FromKind>
  void castNumericUpcast(
      const SelectivityVector& rows,
      const TypePtr& toType,
      exec::EvalCtx& context,
      const BaseVector& input,
      VectorPtr& result);

  bool isTryCast() const {
    return isTryCast_;
  }

  bool setNullInResultAtError() const {
    return isTryCast() && (inTopLevel || hooks_->applyTryCastRecursively());
  }

  /// Helper function to set error status or null in result based on cast
  /// policy. Centralizes error handling logic used across different cast
  /// operations.
  /// @param row The row index where the error occurred
  /// @param context The evaluation context
  /// @param result The result vector to update
  /// @param wrapException Output parameter indicating if exception should be
  /// wrapped
  /// @param details Optional error details message
  template <typename TResult>
  void setCastError(
      vector_size_t row,
      EvalCtx& context,
      TResult* result,
      bool& wrapException,
      const std::string& details = "") const {
    if (setNullInResultAtError()) {
      result->setNull(row, true);
    } else {
      wrapException = false;
      if (context.captureErrorDetails()) {
        if (!details.empty()) {
          context.setStatus(row, Status::UserError("{}", details));
        } else {
          context.setStatus(row, Status::UserError());
        }
      } else {
        context.setStatus(row, Status::UserError());
      }
    }
  }

  /// Helper to set result or error from Expected<T> cast result.
  /// If castResult has an error, sets the error in context. Otherwise, sets
  /// the value in castResult directly to result.
  /// @param row The row index being processed
  /// @param castResult The Expected<T> result from cast operation
  /// @param makeErrorDetails Builds the full error details string lazily from
  /// the cast error message.
  /// @param context The evaluation context
  /// @param result The result vector to update
  /// @param wrapException Output parameter indicating if exception should be
  /// wrapped
  template <typename T, typename TResult, typename TMakeErrorDetails>
  void setResultOrError(
      vector_size_t row,
      const Expected<T>& castResult,
      TMakeErrorDetails makeErrorDetails,
      EvalCtx& context,
      TResult* result,
      bool& wrapException) const {
    if (castResult.hasError()) {
      if (setNullInResultAtError()) {
        setCastError(row, context, result, wrapException);
        return;
      }
      const auto errorDetails = context.captureErrorDetails()
          ? makeErrorDetails(castResult.error().message())
          : std::string{};
      setCastError(row, context, result, wrapException, errorDetails);
    } else {
      result->set(row, castResult.value());
    }
  }

  CastOperatorPtr getCastOperator(const TypePtr& type);

  // Custom cast operators for to and from top-level as well as nested types.
  folly::F14FastMap<std::string, CastOperatorPtr> castOperators_;

  bool isTryCast_;
  std::shared_ptr<CastHooks> hooks_;

  bool inTopLevel = false;
};

// Note: V1's CastCallToSpecialForm / TryCastCallToSpecialForm
// registration helpers are intentionally not duplicated here.  The
// compiler still produces V1 CastExpr instances; CastExprV2 is
// constructed by the V2 adapter (or future V2 compiler path) from the
// V1 CastExpr's state.

} // namespace facebook::velox::exec

#include "velox/expression/CastExprV2-inl.h"
