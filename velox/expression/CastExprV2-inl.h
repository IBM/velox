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

#include "velox/common/base/CountBits.h"
#include "velox/common/base/Exceptions.h"
#include "velox/core/CoreTypeSystem.h"
#include "velox/expression/CastExprV2.h"
#include "velox/expression/StringWriter.h"
#include "velox/functions/lib/string/StringCore.h"
#include "velox/type/Type.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::exec {

// The anonymous-namespace helpers (makeErrorMessage,
// makeBadCastException, isSupportedFastUpcast) live in
// CastExpr-inl.h, which is transitively included via CastExpr.h.
// Reusing them avoids an ODR violation in translation units that
// include both inl headers.

template <typename Func>
void CastExprV2::applyToSelectedNoThrowLocal(
    EvalCtx& context,
    const SelectivityVector& rows,
    VectorPtr& result,
    Func&& func) {
  if (setNullInResultAtError()) {
    rows.applyToSelected([&](auto row) INLINE_LAMBDA {
      try {
        func(row);
      } catch (const VeloxException& e) {
        if (!e.isUserError()) {
          throw;
        }
        result->setNull(row, true);
      } catch (const std::exception&) {
        result->setNull(row, true);
      }
    });
  } else {
    rows.applyToSelected([&](auto row) INLINE_LAMBDA {
      try {
        func(row);
      } catch (const VeloxException& e) {
        if (!e.isUserError()) {
          throw;
        }
        // Avoid double throwing.
        context.setVeloxExceptionError(row, std::current_exception());
      } catch (const std::exception&) {
        context.setError(row, std::current_exception());
      }
    });
  }
}

/// The per-row level Kernel
/// @tparam ToKind The cast target type
/// @tparam FromKind The expression type
/// @tparam TPolicy The policy used by the cast
/// @param row The index of the current row
/// @param input The input vector (of type FromKind)
/// @param result The output vector (of type ToKind)
template <TypeKind ToKind, TypeKind FromKind, typename TPolicy>
void CastExprV2::castKernel(
    vector_size_t row,
    EvalCtx& context,
    const SimpleVector<typename TypeTraits<FromKind>::NativeType>* input,
    FlatVector<typename TypeTraits<ToKind>::NativeType>* result) {
  bool wrapException = true;
  auto setError = [&](const std::string& details) INLINE_LAMBDA {
    if (setNullInResultAtError()) {
      setCastError(row, context, result, wrapException);
      return;
    }
    const auto errorDetails = context.captureErrorDetails()
        ? makeErrorMessage(*input, row, result->type(), details)
        : std::string{};
    setCastError(row, context, result, wrapException, errorDetails);
  };

  try {
    auto inputRowValue = input->valueAt(row);

    // Optimize empty input strings casting by avoiding throwing exceptions.
    // VARCHAR -> hook-served primitive kinds (TIMESTAMP, DATE, REAL, DOUBLE)
    // are vectorized through castPrimitives and never reach this kernel;
    // what remains here is VARCHAR -> INT/HUGEINT/BOOLEAN handled by
    // util::Converter below.
    if constexpr (is_string_kind(FromKind)) {
      if constexpr (
          TypeTraits<ToKind>::isPrimitiveType &&
          TypeTraits<ToKind>::isFixedWidth) {
        inputRowValue = hooks_->removeWhiteSpaces(inputRowValue);
        if (inputRowValue.size() == 0) {
          setError("Empty string");
          return;
        }
      }
      if constexpr (
          ToKind == TypeKind::TINYINT || ToKind == TypeKind::SMALLINT ||
          ToKind == TypeKind::INTEGER || ToKind == TypeKind::BIGINT ||
          ToKind == TypeKind::HUGEINT) {
        if constexpr (TPolicy::throwOnUnicode) {
          if (!functions::stringCore::isAscii(
                  inputRowValue.data(), inputRowValue.size())) {
            VELOX_USER_FAIL(
                "Unicode characters are not supported for conversion to integer types");
          }
        }
      }
    }

    const auto castResult =
        util::Converter<ToKind, void, TPolicy>::tryCast(inputRowValue);
    if (castResult.hasError()) {
      setError(castResult.error().message());
      return;
    }

    const auto& output = castResult.value();

    if constexpr (is_string_kind(ToKind)) {
      // Write the result output to the output vector
      auto writer = exec::StringWriter(result, row);
      writer.copy_from(output);
      writer.finalize();
    } else {
      result->set(row, output);
    }

  } catch (const VeloxException& ue) {
    if (!ue.isUserError() || !wrapException) {
      throw;
    }
    setError(ue.message());
  } catch (const std::exception& e) {
    setError(e.what());
  }
}

template <typename TInput, typename TOutput>
void CastExprV2::castDecimalToDecimalKernel(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType,
    VectorPtr& castResult) {
  auto sourceVector = input.as<SimpleVector<TInput>>();
  auto castResultRawBuffer =
      castResult->asUnchecked<FlatVector<TOutput>>()->mutableRawValues();
  const auto& fromPrecisionScale = getDecimalPrecisionScale(*fromType);
  const auto& toPrecisionScale = getDecimalPrecisionScale(*toType);

  applyToSelectedNoThrowLocal(
      context, rows, castResult, [&](vector_size_t row) {
        TOutput rescaledValue;
        const auto status = DecimalUtil::rescaleWithRoundUp<TInput, TOutput>(
            sourceVector->valueAt(row),
            fromPrecisionScale.first,
            fromPrecisionScale.second,
            toPrecisionScale.first,
            toPrecisionScale.second,
            rescaledValue);
        if (status.ok()) {
          castResultRawBuffer[row] = rescaledValue;
        } else {
          if (setNullInResultAtError()) {
            castResult->setNull(row, true);
          } else {
            context.setVeloxExceptionError(
                row,
                std::make_exception_ptr(VeloxUserError(
                    std::current_exception(), status.message(), false)));
          }
        }
      });
}

template <typename TInput, typename TOutput>
void CastExprV2::castIntToDecimalKernel(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& toType,
    VectorPtr& castResult) {
  auto sourceVector = input.as<SimpleVector<TInput>>();
  auto castResultRawBuffer =
      castResult->asUnchecked<FlatVector<TOutput>>()->mutableRawValues();
  const auto& toPrecisionScale = getDecimalPrecisionScale(*toType);
  applyToSelectedNoThrowLocal(
      context, rows, castResult, [&](vector_size_t row) {
        auto rescaledValue = DecimalUtil::rescaleInt<TInput, TOutput>(
            sourceVector->valueAt(row),
            toPrecisionScale.first,
            toPrecisionScale.second);
        if (rescaledValue.has_value()) {
          castResultRawBuffer[row] = rescaledValue.value();
        } else {
          castResult->setNull(row, true);
        }
      });
}

template <typename TInput, typename TOutput>
void CastExprV2::castFloatToDecimalKernel(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& toType,
    VectorPtr& result) {
  const auto floatingInput = input.as<SimpleVector<TInput>>();
  auto rawResults =
      result->asUnchecked<FlatVector<TOutput>>()->mutableRawValues();
  const auto toPrecisionScale = getDecimalPrecisionScale(*toType);

  applyToSelectedNoThrowLocal(context, rows, result, [&](vector_size_t row) {
    TOutput output;
    const auto status = DecimalUtil::rescaleFloatingPoint<TInput, TOutput>(
        floatingInput->valueAt(row),
        toPrecisionScale.first,
        toPrecisionScale.second,
        output);
    if (status.ok()) {
      rawResults[row] = output;
    } else {
      if (setNullInResultAtError()) {
        result->setNull(row, true);
      } else {
        context.setVeloxExceptionError(
            row, makeBadCastException(toType, input, row, status.message()));
      }
    }
  });
}

template <typename T>
void CastExprV2::castVarcharToDecimalKernel(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& toType,
    VectorPtr& result) {
  auto sourceVector = input.as<SimpleVector<StringView>>();
  auto rawBuffer = result->asUnchecked<FlatVector<T>>()->mutableRawValues();
  const auto toPrecisionScale = getDecimalPrecisionScale(*toType);

  rows.applyToSelected([&](auto row) {
    T decimalValue;
    const auto status = DecimalUtil::castFromString<T>(
        hooks_->removeWhiteSpaces(sourceVector->valueAt(row)),
        toPrecisionScale.first,
        toPrecisionScale.second,
        decimalValue);
    if (status.ok()) {
      rawBuffer[row] = decimalValue;
    } else {
      if (setNullInResultAtError()) {
        result->setNull(row, true);
      } else {
        context.setVeloxExceptionError(
            row, makeBadCastException(toType, input, row, status.message()));
      }
    }
  });
}

template <typename FromNativeType, TypeKind ToKind>
VectorPtr CastExprV2::castDecimalToFloat(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType) {
  using To = typename TypeTraits<ToKind>::NativeType;

  VectorPtr result;
  context.ensureWritable(rows, toType, result);
  (*result).clearNulls(rows);
  auto resultBuffer = result->asUnchecked<FlatVector<To>>()->mutableRawValues();
  const auto precisionScale = getDecimalPrecisionScale(*fromType);
  const auto simpleInput = input.as<SimpleVector<FromNativeType>>();
  const auto scaleFactor = DecimalUtil::kPowersOfTen[precisionScale.second];
  applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
    const auto unscaledValue = simpleInput->valueAt(row);
    // Avoid precision loss: float has ~7 significant digits; casting unscaled
    // int128 to float first loses precision for values with 8+ digits (e.g.
    // 113751964). Divide in double then cast to float so result is correct.
    To finalValue;
    if constexpr (ToKind == TypeKind::REAL) {
      const auto output =
          util::Converter<TypeKind::DOUBLE>::tryCast(unscaledValue)
              .thenOrThrow(folly::identity, [&](const Status& status) {
                VELOX_USER_FAIL("{}", status.message());
              });
      finalValue = static_cast<To>(output / scaleFactor);
    } else {
      const auto output =
          util::Converter<ToKind>::tryCast(unscaledValue)
              .thenOrThrow(folly::identity, [&](const Status& status) {
                VELOX_USER_FAIL("{}", status.message());
              });
      finalValue = output / scaleFactor;
    }
    resultBuffer[row] = finalValue;
  });
  return result;
}

template <typename FromNativeType, TypeKind ToKind>
VectorPtr CastExprV2::castDecimalToIntegral(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType) {
  using To = typename TypeTraits<ToKind>::NativeType;

  VectorPtr result;
  context.ensureWritable(rows, toType, result);
  (*result).clearNulls(rows);
  auto resultBuffer = result->asUnchecked<FlatVector<To>>()->mutableRawValues();
  const auto precisionScale = getDecimalPrecisionScale(*fromType);
  const auto simpleInput = input.as<SimpleVector<FromNativeType>>();
  const auto scaleFactor = DecimalUtil::kPowersOfTen[precisionScale.second];
  if (hooks_->truncate()) {
    applyToSelectedNoThrowLocal(context, rows, result, [&](vector_size_t row) {
      resultBuffer[row] =
          static_cast<To>(simpleInput->valueAt(row) / scaleFactor);
    });
  } else {
    applyToSelectedNoThrowLocal(context, rows, result, [&](vector_size_t row) {
      auto value = simpleInput->valueAt(row);
      auto integralPart = value / scaleFactor;
      if (hooks_->getPolicy() != SparkTryCastPolicy) {
        auto fractionPart = value % scaleFactor;
        auto sign = value >= 0 ? 1 : -1;
        bool needsRoundUp =
            (scaleFactor != 1) && (sign * fractionPart >= (scaleFactor >> 1));
        integralPart += needsRoundUp ? sign : 0;
      }

      if (integralPart > std::numeric_limits<To>::max() ||
          integralPart < std::numeric_limits<To>::min()) {
        if (setNullInResultAtError()) {
          result->setNull(row, true);
        } else {
          context.setVeloxExceptionError(
              row,
              makeBadCastException(
                  result->type(), input, row, "Out of bounds."));
        }
        return;
      }

      resultBuffer[row] = static_cast<To>(integralPart);
    });
  }
  return result;
}

template <typename FromNativeType>
VectorPtr CastExprV2::castDecimalToBoolean(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context) {
  VectorPtr result;
  context.ensureWritable(rows, BOOLEAN(), result);
  (*result).clearNulls(rows);
  auto resultBuffer =
      result->asUnchecked<FlatVector<bool>>()->mutableRawValues<uint64_t>();
  const auto simpleInput = input.as<SimpleVector<FromNativeType>>();
  applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
    auto value = simpleInput->valueAt(row);
    bits::setBit(resultBuffer, row, value != 0);
  });
  return result;
}

template <typename FromNativeType>
VectorPtr CastExprV2::castDecimalToVarchar(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType) {
  VectorPtr result;
  context.ensureWritable(rows, VARCHAR(), result);
  (*result).clearNulls(rows);
  const auto simpleInput = input.as<SimpleVector<FromNativeType>>();
  int precision = getDecimalPrecisionScale(*fromType).first;
  int scale = getDecimalPrecisionScale(*fromType).second;
  auto rowSize = DecimalUtil::maxStringViewSize(precision, scale);
  auto flatResult = result->asFlatVector<StringView>();
  if (StringView::isInline(rowSize)) {
    char inlined[StringView::kInlineSize];
    applyToSelectedNoThrowLocal(context, rows, result, [&](vector_size_t row) {
      auto actualSize = DecimalUtil::castToString<FromNativeType>(
          simpleInput->valueAt(row),
          scale,
          rowSize,
          inlined,
          hooks_->isScientific());
      flatResult->setNoCopy(row, StringView(inlined, actualSize));
    });
    return result;
  }

  Buffer* buffer =
      flatResult->getBufferWithSpace(rows.countSelected() * rowSize);
  char* rawBuffer = buffer->asMutable<char>() + buffer->size();

  applyToSelectedNoThrowLocal(context, rows, result, [&](vector_size_t row) {
    auto actualSize = DecimalUtil::castToString<FromNativeType>(
        simpleInput->valueAt(row),
        scale,
        rowSize,
        rawBuffer,
        hooks_->isScientific());
    flatResult->setNoCopy(row, StringView(rawBuffer, actualSize));
    if (!StringView::isInline(actualSize)) {
      // If string view is inline, corresponding bytes on the raw string buffer
      // are not needed.
      rawBuffer += actualSize;
    }
  });
  // Update the exact buffer size.
  buffer->setSize(rawBuffer - buffer->asMutable<char>());
  return result;
}

template <typename FromNativeType>
VectorPtr CastExprV2::castDecimalToPrimitive(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType) {
  switch (toType->kind()) {
    case TypeKind::BOOLEAN:
      return castDecimalToBoolean<FromNativeType>(rows, input, context);
    case TypeKind::TINYINT:
      return castDecimalToIntegral<FromNativeType, TypeKind::TINYINT>(
          rows, input, context, fromType, toType);
    case TypeKind::SMALLINT:
      return castDecimalToIntegral<FromNativeType, TypeKind::SMALLINT>(
          rows, input, context, fromType, toType);
    case TypeKind::INTEGER:
      return castDecimalToIntegral<FromNativeType, TypeKind::INTEGER>(
          rows, input, context, fromType, toType);
    case TypeKind::BIGINT:
      return castDecimalToIntegral<FromNativeType, TypeKind::BIGINT>(
          rows, input, context, fromType, toType);
    case TypeKind::REAL:
      return castDecimalToFloat<FromNativeType, TypeKind::REAL>(
          rows, input, context, fromType, toType);
    case TypeKind::DOUBLE:
      return castDecimalToFloat<FromNativeType, TypeKind::DOUBLE>(
          rows, input, context, fromType, toType);
    default:
      VELOX_UNSUPPORTED(
          "Cast from {} to {} is not supported",
          fromType->toString(),
          toType->toString());
  }
}

template <TypeKind ToKind, TypeKind FromKind>
void CastExprV2::castPrimitives(
    const SelectivityVector& rows,
    exec::EvalCtx& context,
    const BaseVector& input,
    VectorPtr& result) {
  using To = typename TypeTraits<ToKind>::NativeType;
  using From = typename TypeTraits<FromKind>::NativeType;
  auto* resultFlatVector = result->as<FlatVector<To>>();
  auto* inputSimpleVector = input.as<SimpleVector<From>>();

  // Hook-served paths dispatch as whole-column vector calls so the
  // concrete CastHooksV2 implementation can devirtualize and inline
  // the scalar parser inside its own row loop.  INT (any width),
  // BOOLEAN, DOUBLE/REAL -> TIMESTAMP and TIMESTAMP -> INT (any width)
  // are policy-disallowed in Presto today and record errors in bulk;
  // they reach this dispatch unchanged for dialects that allow them.
  if constexpr (
      (FromKind == TypeKind::TINYINT || FromKind == TypeKind::SMALLINT ||
       FromKind == TypeKind::INTEGER || FromKind == TypeKind::BIGINT) &&
      ToKind == TypeKind::TIMESTAMP) {
    hooks_->castIntToTimestampVector(rows, context);
    return;
  }
  if constexpr (FromKind == TypeKind::BOOLEAN && ToKind == TypeKind::TIMESTAMP) {
    hooks_->castBooleanToTimestampVector(rows, context);
    return;
  }
  if constexpr (
      (ToKind == TypeKind::TINYINT || ToKind == TypeKind::SMALLINT ||
       ToKind == TypeKind::INTEGER || ToKind == TypeKind::BIGINT) &&
      FromKind == TypeKind::TIMESTAMP) {
    hooks_->castTimestampToIntVector(rows, context);
    return;
  }
  if constexpr (
      (FromKind == TypeKind::DOUBLE || FromKind == TypeKind::REAL) &&
      ToKind == TypeKind::TIMESTAMP) {
    hooks_->castDoubleToTimestampVector(rows, context);
    return;
  }

  // VARCHAR -> {TIMESTAMP, REAL, DOUBLE} all flow through the vector
  // hooks; whitespace stripping and empty-string error reporting
  // happen inside the hook to match V1's per-row preprocessing.
  if constexpr (
      FromKind == TypeKind::VARCHAR && ToKind == TypeKind::TIMESTAMP) {
    hooks_->castStringToTimestampVector(
        rows, *inputSimpleVector, *resultFlatVector, context);
    return;
  }
  if constexpr (FromKind == TypeKind::VARCHAR && ToKind == TypeKind::REAL) {
    hooks_->castStringToRealVector(
        rows, *inputSimpleVector, *resultFlatVector, context);
    return;
  }
  if constexpr (FromKind == TypeKind::VARCHAR && ToKind == TypeKind::DOUBLE) {
    hooks_->castStringToDoubleVector(
        rows, *inputSimpleVector, *resultFlatVector, context);
    return;
  }

  switch (hooks_->getPolicy()) {
    case LegacyCastPolicy:
      applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
        castKernel<ToKind, FromKind, util::LegacyCastPolicy>(
            row, context, inputSimpleVector, resultFlatVector);
      });
      break;
    case PrestoCastPolicy:
      applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
        castKernel<ToKind, FromKind, util::PrestoCastPolicy>(
            row, context, inputSimpleVector, resultFlatVector);
      });
      break;
    case SparkCastPolicy:
      applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
        castKernel<ToKind, FromKind, util::SparkCastPolicy>(
            row, context, inputSimpleVector, resultFlatVector);
      });
      break;
    case SparkTryCastPolicy:
      applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
        castKernel<ToKind, FromKind, util::SparkTryCastPolicy>(
            row, context, inputSimpleVector, resultFlatVector);
      });
      break;

    default:
      VELOX_NYI("Policy {} not yet implemented.", hooks_->getPolicy());
  }
}

template <TypeKind ToKind, TypeKind FromKind>
void CastExprV2::castNumericUpcast(
    const SelectivityVector& rows,
    const TypePtr& toType,
    exec::EvalCtx& context,
    const BaseVector& input,
    VectorPtr& result) {
  constexpr auto isNumericTypeKind = [](TypeKind kind) constexpr {
    return kind == TypeKind::TINYINT || kind == TypeKind::SMALLINT ||
        kind == TypeKind::INTEGER || kind == TypeKind::BIGINT ||
        kind == TypeKind::REAL || kind == TypeKind::DOUBLE;
  };

  if constexpr (isNumericTypeKind(ToKind) && isNumericTypeKind(FromKind)) {
    using ToNativeType = typename TypeTraits<ToKind>::NativeType;
    using FromNativeType = typename TypeTraits<FromKind>::NativeType;

    if (input.isConstantEncoding()) {
      auto constantInput = input.as<ConstantVector<FromNativeType>>();
      if (constantInput->isNullAt(0)) {
        result =
            BaseVector::createNullConstant(toType, rows.end(), context.pool());
        return;
      }
      auto constantValue = static_cast<ToNativeType>(constantInput->valueAt(0));
      result = std::make_shared<ConstantVector<ToNativeType>>(
          context.pool(),
          rows.end(),
          /*isNull=*/false,
          toType,
          std::move(constantValue));
      return;
    }

    if (input.isFlatEncoding()) {
      const auto simpleInput = input.asFlatVector<FromNativeType>();
      auto flatResult = result->asFlatVector<ToNativeType>();

      const FromNativeType* in =
          simpleInput->template rawValues<FromNativeType>();
      ToNativeType* out = flatResult->template mutableRawValues<ToNativeType>();

      rows.applyToSelected([&](auto row) {
        // Converting large bigint values to float/double directly may lose
        // precision, but it's consistent with the implementation in
        // velox/type/Conversions.h.
        out[row] = static_cast<ToNativeType>(in[row]);
      });
      return;
    }
  }
  VELOX_UNSUPPORTED(
      "Cannot upcast from {} to {}", input.type(), toType->toString());
}

template <TypeKind ToKind>
void CastExprV2::castPrimitivesDispatch(
    const TypePtr& fromType,
    const TypePtr& toType,
    const SelectivityVector& rows,
    exec::EvalCtx& context,
    const BaseVector& input,
    VectorPtr& result) {
  context.ensureWritable(rows, toType, result);

  if (fromType->kind() == TypeKind::TIMESTAMP) {
    VELOX_DCHECK(fromType->equivalent(*TIMESTAMP()));
  }

  if constexpr (ToKind == TypeKind::TIMESTAMP) {
    VELOX_DCHECK(toType->equivalent(*TIMESTAMP()));
  }

  if (isSupportedFastUpcast(fromType, toType)) {
    VELOX_DYNAMIC_SCALAR_TEMPLATE_TYPE_DISPATCH(
        castNumericUpcast,
        ToKind,
        fromType->kind(),
        rows,
        toType,
        context,
        input,
        result);
    return;
  }

  // This already excludes complex types, hugeint and unknown from type kinds.
  VELOX_DYNAMIC_SCALAR_TEMPLATE_TYPE_DISPATCH(
      castPrimitives,
      ToKind,
      fromType->kind() /*dispatched*/,
      rows,
      context,
      input,
      result);
}

} // namespace facebook::velox::exec
