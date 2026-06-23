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

#include "velox/expression/CastExprV2.h"

#include <fmt/format.h>
#include <stdexcept>

#include "velox/common/base/Exceptions.h"
#include "velox/core/CoreTypeSystem.h"
#include "velox/expression/PeeledEncoding.h"
#include "velox/expression/PrestoCastHooksV2.h"
#include "velox/expression/ScopedVarSetter.h"
#include "velox/external/tzdb/time_zone.h"
#include "velox/functions/lib/RowsTranslationUtil.h"
#include "velox/type/CastRegistry.h"
#include "velox/type/Type.h"
#include "velox/type/tz/TimeZoneMap.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FunctionVector.h"
#include "velox/vector/LazyVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::exec {

namespace {

// Default V2 cast hooks factory: PrestoCastHooksV2.  Composes a
// PrestoCastHooks internally so behavior matches V1 row-for-row;
// callers register a different factory via CastExprV2::setHooksFactory
// to plug in another CastHooksV2 implementation (e.g., a Spark variant).
std::shared_ptr<CastHooksV2> makeDefaultV2CastHooks(
    const core::QueryConfig& config,
    bool /*isTryCast*/) {
  return std::make_shared<PrestoCastHooksV2>(config);
}

// Storage for the registered V2 cast hooks factory.  Wrapped in a
// function-local static so the order of initialization with any
// other globals doesn't matter.
CastExprV2::HooksFactory& globalHooksFactory() {
  static CastExprV2::HooksFactory factory = &makeDefaultV2CastHooks;
  return factory;
}

const tz::TimeZone* getTimeZoneFromConfig(const core::QueryConfig& config) {
  if (config.adjustTimestampToTimezone()) {
    const auto sessionTzName = config.sessionTimezone();
    if (!sessionTzName.empty()) {
      return tz::locateZone(sessionTzName);
    }
  }
  return nullptr;
}

} // namespace

VectorPtr CastExprV2::castFromDate(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& toType) {
  VectorPtr castResult;
  context.ensureWritable(rows, toType, castResult);
  (*castResult).clearNulls(rows);

  auto* inputFlatVector = input.as<SimpleVector<int32_t>>();
  switch (toType->kind()) {
    case TypeKind::VARCHAR: {
      auto* resultFlatVector = castResult->as<FlatVector<StringView>>();
      applyToSelectedNoThrowLocal(context, rows, castResult, [&](int row) {
        try {
          // TODO Optimize to avoid creating an intermediate string.
          auto output = DATE()->toString(inputFlatVector->valueAt(row));
          auto writer = exec::StringWriter(resultFlatVector, row);
          writer.resize(output.size());
          ::memcpy(writer.data(), output.data(), output.size());
          writer.finalize();
        } catch (const VeloxException& ue) {
          if (!ue.isUserError()) {
            throw;
          }
          VELOX_USER_FAIL(
              makeErrorMessage(input, row, toType) + " " + ue.message());
        } catch (const std::exception& e) {
          VELOX_USER_FAIL(
              makeErrorMessage(input, row, toType) + " " + e.what());
        }
      });
      return castResult;
    }
    case TypeKind::TIMESTAMP: {
      VELOX_DCHECK(toType->equivalent(*TIMESTAMP()));
      const auto* timeZone =
          getTimeZoneFromConfig(context.execCtx()->queryCtx()->queryConfig());
      auto* resultFlatVector = castResult->as<FlatVector<Timestamp>>();
      hooks_->castDateToTimestampVector(
          rows, *inputFlatVector, *resultFlatVector, timeZone);
      return castResult;
    }
    default:
      VELOX_UNSUPPORTED(
          "Cast from DATE to {} is not supported", toType->toString());
  }
}

VectorPtr CastExprV2::castToDate(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType) {
  VectorPtr castResult;
  context.ensureWritable(rows, DATE(), castResult);
  (*castResult).clearNulls(rows);
  auto* resultFlatVector = castResult->as<FlatVector<int32_t>>();
  switch (fromType->kind()) {
    case TypeKind::VARCHAR: {
      auto* inputVector = input.as<SimpleVector<StringView>>();
      hooks_->castStringToDateVector(rows, *inputVector, *resultFlatVector, context);
      return castResult;
    }
    case TypeKind::TIMESTAMP: {
      VELOX_DCHECK(fromType->equivalent(*TIMESTAMP()));
      auto* inputVector = input.as<SimpleVector<Timestamp>>();
      const auto* timeZone =
          getTimeZoneFromConfig(context.execCtx()->queryCtx()->queryConfig());
      applyToSelectedNoThrowLocal(context, rows, castResult, [&](int row) {
        const auto days = util::toDate(inputVector->valueAt(row), timeZone);
        resultFlatVector->set(row, days);
      });
      return castResult;
    }
    default:
      VELOX_UNSUPPORTED(
          "Cast from {} to DATE is not supported", fromType->toString());
  }
}

VectorPtr CastExprV2::castFromIntervalDayTime(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& toType) {
  VectorPtr castResult;
  context.ensureWritable(rows, toType, castResult);
  (*castResult).clearNulls(rows);

  auto* inputFlatVector = input.as<SimpleVector<int64_t>>();
  switch (toType->kind()) {
    case TypeKind::VARCHAR: {
      auto* resultFlatVector = castResult->as<FlatVector<StringView>>();
      applyToSelectedNoThrowLocal(context, rows, castResult, [&](int row) {
        try {
          // TODO Optimize to avoid creating an intermediate string.
          auto output =
              INTERVAL_DAY_TIME()->valueToString(inputFlatVector->valueAt(row));
          auto writer = exec::StringWriter(resultFlatVector, row);
          writer.resize(output.size());
          ::memcpy(writer.data(), output.data(), output.size());
          writer.finalize();
        } catch (const VeloxException& ue) {
          if (!ue.isUserError()) {
            throw;
          }
          VELOX_USER_FAIL(
              makeErrorMessage(input, row, toType) + " " + ue.message());
        } catch (const std::exception& e) {
          VELOX_USER_FAIL(
              makeErrorMessage(input, row, toType) + " " + e.what());
        }
      });
      return castResult;
    }
    default:
      VELOX_UNSUPPORTED(
          "Cast from {} to {} is not supported",
          INTERVAL_DAY_TIME()->toString(),
          toType->toString());
  }
}

VectorPtr CastExprV2::castFromTime(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& toType) {
  VectorPtr castResult;
  context.ensureWritable(rows, toType, castResult);
  (*castResult).clearNulls(rows);

  auto* inputFlatVector = input.as<SimpleVector<int64_t>>();
  switch (toType->kind()) {
    case TypeKind::VARCHAR: {
      // Get session timezone
      const auto* timeZone =
          getTimeZoneFromConfig(context.execCtx()->queryCtx()->queryConfig());
      // Get session start time
      const auto startTimeMs =
          context.execCtx()->queryCtx()->queryConfig().sessionStartTimeMs();
      auto systemDay = std::chrono::milliseconds{startTimeMs} / kMillisInDay;

      auto* resultFlatVector = castResult->as<FlatVector<StringView>>();

      Buffer* buffer = resultFlatVector->getBufferWithSpace(
          rows.countSelected() * TIME()->timeToVarcharRowSize(),
          true /*exactSize*/);
      char* rawBuffer = buffer->asMutable<char>() + buffer->size();

      // Hoist the timezone-presence branch out of the per-row loop:
      // pick one of two specialized loops here, so the inner body has
      // no condition to evaluate for every row.
      auto rowBody = [&](int row, int64_t adjustedTime) {
        try {
          if (adjustedTime < 0) {
            adjustedTime += kMillisInDay;
          }
          auto output = TIME()->valueToString(adjustedTime, rawBuffer);
          resultFlatVector->setNoCopy(row, output);
          rawBuffer += output.size();
        } catch (const VeloxException& ue) {
          if (!ue.isUserError()) {
            throw;
          }
          VELOX_USER_FAIL(
              makeErrorMessage(input, row, toType) + " " + ue.message());
        } catch (const std::exception& e) {
          VELOX_USER_FAIL(
              makeErrorMessage(input, row, toType) + " " + e.what());
        }
      };
      if (timeZone != nullptr) {
        applyToSelectedNoThrowLocal(context, rows, castResult, [&](int row) {
          const auto systemTime =
              systemDay.count() * kMillisInDay + inputFlatVector->valueAt(row);
          const int64_t adjustedTime =
              (timeZone->to_local(std::chrono::milliseconds{systemTime}) %
               kMillisInDay)
                  .count();
          rowBody(row, adjustedTime);
        });
      } else {
        applyToSelectedNoThrowLocal(context, rows, castResult, [&](int row) {
          const auto systemTime =
              systemDay.count() * kMillisInDay + inputFlatVector->valueAt(row);
          rowBody(row, systemTime % kMillisInDay);
        });
      }

      buffer->setSize(rawBuffer - buffer->asMutable<char>());
      return castResult;
    }
    case TypeKind::BIGINT: {
      // if input is constant, create a constant output vector
      if (input.isConstantEncoding()) {
        auto constantInput = input.as<ConstantVector<int64_t>>();
        if (constantInput->isNullAt(0)) {
          return BaseVector::createNullConstant(
              toType, rows.end(), context.pool());
        } else {
          auto constantValue = constantInput->valueAt(0);
          return std::make_shared<ConstantVector<int64_t>>(
              context.pool(),
              rows.end(),
              false, // isNull
              toType,
              std::move(constantValue));
        }
      }

      // fallback to element-wise copy for non-constant inputs
      auto* resultFlatVector = castResult->as<FlatVector<int64_t>>();
      applyToSelectedNoThrowLocal(context, rows, castResult, [&](int row) {
        resultFlatVector->set(row, inputFlatVector->valueAt(row));
      });
      return castResult;
    }
    case TypeKind::TIMESTAMP: {
      VELOX_DCHECK(toType->equivalent(*TIMESTAMP()));
      // if input is constant, create a constant output vector
      if (input.isConstantEncoding()) {
        auto constantInput = input.as<ConstantVector<int64_t>>();
        if (constantInput->isNullAt(0)) {
          return BaseVector::createNullConstant(
              toType, rows.end(), context.pool());
        } else {
          auto timeMillis = constantInput->valueAt(0);
          return std::make_shared<ConstantVector<Timestamp>>(
              context.pool(),
              rows.end(),
              false, // isNull
              toType,
              Timestamp::fromMillis(timeMillis));
        }
      }

      // fallback to element-wise copy for non-constant inputs
      auto* resultFlatVector = castResult->as<FlatVector<Timestamp>>();
      applyToSelectedNoThrowLocal(context, rows, castResult, [&](int row) {
        auto timeMillis = inputFlatVector->valueAt(row);
        resultFlatVector->set(row, Timestamp::fromMillis(timeMillis));
      });
      return castResult;
    }
    default:
      VELOX_UNSUPPORTED(
          "Cast from TIME to {} is not supported", toType->toString());
  }
}

VectorPtr CastExprV2::castToTime(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType) {
  switch (fromType->kind()) {
    case TypeKind::VARCHAR: {
      VectorPtr castResult;
      context.ensureWritable(rows, TIME(), castResult);
      (*castResult).clearNulls(rows);

      // Get session timezone and start time for timezone conversions
      const auto* timeZone =
          getTimeZoneFromConfig(context.execCtx()->queryCtx()->queryConfig());
      const auto sessionStartTimeMs =
          context.execCtx()->queryCtx()->queryConfig().sessionStartTimeMs();

      auto* inputVector = input.as<SimpleVector<StringView>>();
      auto* resultFlatVector = castResult->as<FlatVector<int64_t>>();

      applyToSelectedNoThrowLocal(context, rows, castResult, [&](int row) {
        try {
          const auto inputString = inputVector->valueAt(row);
          int64_t result =
              TIME()->valueToTime(inputString, timeZone, sessionStartTimeMs);
          resultFlatVector->set(row, result);
        } catch (const VeloxException& ue) {
          if (!ue.isUserError()) {
            throw;
          }
          VELOX_USER_FAIL(
              makeErrorMessage(input, row, TIME()) + " " + ue.message());
        } catch (const std::exception& e) {
          VELOX_USER_FAIL(
              makeErrorMessage(input, row, TIME()) + " " + e.what());
        }
      });

      return castResult;
    }
    case TypeKind::TIMESTAMP: {
      VELOX_DCHECK(fromType->equivalent(*TIMESTAMP()));
      VectorPtr castResult;
      context.ensureWritable(rows, TIME(), castResult);
      (*castResult).clearNulls(rows);

      auto* inputVector = input.as<SimpleVector<Timestamp>>();
      auto* resultFlatVector = castResult->as<FlatVector<int64_t>>();

      // Cast from TIMESTAMP to TIME extracts the time-of-day component
      // (milliseconds since midnight) from the timestamp
      applyToSelectedNoThrowLocal(context, rows, castResult, [&](int row) {
        const auto timestamp = inputVector->valueAt(row);
        // Extract time-of-day using std::chrono.
        // floor() also rounds towards negative infinity, so this correctly
        // handles negative timestamps.
        auto millis = std::chrono::milliseconds{timestamp.toMillis()};
        auto timeOfDay = millis - std::chrono::floor<std::chrono::days>(millis);
        resultFlatVector->set(row, timeOfDay.count());
      });

      return castResult;
    }
    default:
      VELOX_UNSUPPORTED(
          "Cast from {} to TIME is not supported", fromType->toString());
  }
}

namespace {
void propagateErrorsOrSetNulls(
    bool setNullInResultAtError,
    EvalCtx& context,
    const SelectivityVector& nestedRows,
    const BufferPtr& elementToTopLevelRows,
    VectorPtr& result,
    exec::EvalErrorsPtr& oldErrors) {
  if (context.errors()) {
    if (setNullInResultAtError) {
      // Errors in context.errors() should be translated to nulls in
      // the top level rows.
      context.convertElementErrorsToTopLevelNulls(
          nestedRows, elementToTopLevelRows, result);
    } else {
      context.addElementErrorsToTopLevel(
          nestedRows, elementToTopLevelRows, oldErrors);
    }
  }
}
} // namespace

#define VELOX_DYNAMIC_DECIMAL_TYPE_DISPATCH(       \
    TEMPLATE_FUNC, decimalTypePtr, ...)            \
  [&]() {                                          \
    if (decimalTypePtr->isLongDecimal()) {         \
      return TEMPLATE_FUNC<int128_t>(__VA_ARGS__); \
    } else {                                       \
      return TEMPLATE_FUNC<int64_t>(__VA_ARGS__);  \
    }                                              \
  }()

VectorPtr CastExprV2::castMap(
    const SelectivityVector& rows,
    const MapVector* input,
    exec::EvalCtx& context,
    const MapType& fromType,
    const MapType& toType) {
  // Cast input keys/values vector to output keys/values vector using
  // their element selectivity vector

  // Initialize nested rows
  auto mapKeys = input->mapKeys();
  auto mapValues = input->mapValues();

  SelectivityVector nestedRows;
  BufferPtr elementToTopLevelRows;
  if (fromType.keyType() != toType.keyType() ||
      fromType.valueType() != toType.valueType()) {
    nestedRows = functions::toElementRows(mapKeys->size(), rows, input);
    elementToTopLevelRows = functions::getElementToTopLevelRows(
        mapKeys->size(), rows, input, context.pool());
  }

  EvalErrorsPtr oldErrors;
  context.swapErrors(oldErrors);

  // Cast keys
  VectorPtr newMapKeys;
  if (*fromType.keyType() == *toType.keyType()) {
    newMapKeys = input->mapKeys();
  } else {
    {
      ScopedVarSetter holder(&inTopLevel, false);
      apply(
          nestedRows,
          mapKeys,
          context,
          fromType.keyType(),
          toType.keyType(),
          newMapKeys);
    }
  }

  // Cast values
  VectorPtr newMapValues;
  if (*fromType.valueType() == *toType.valueType()) {
    newMapValues = mapValues;
  } else {
    {
      ScopedVarSetter holder(&inTopLevel, false);
      apply(
          nestedRows,
          mapValues,
          context,
          fromType.valueType(),
          toType.valueType(),
          newMapValues);
    }
  }

  // Returned map vector should be addressable for every element, even
  // those that are not selected.
  BufferPtr sizes = input->sizes();
  if (newMapKeys->isConstantEncoding() && newMapValues->isConstantEncoding()) {
    // We extends size since that is cheap.
    newMapKeys->resize(input->mapKeys()->size());
    newMapValues->resize(input->mapValues()->size());
  } else if (
      newMapKeys->size() < input->mapKeys()->size() ||
      newMapValues->size() < input->mapValues()->size()) {
    sizes =
        AlignedBuffer::allocate<vector_size_t>(rows.end(), context.pool(), 0);
    auto* inputSizes = input->rawSizes();
    auto* rawSizes = sizes->asMutable<vector_size_t>();

    rows.applyToSelected(
        [&](vector_size_t row) { rawSizes[row] = inputSizes[row]; });
  }

  // Assemble the output map
  VectorPtr result = std::make_shared<MapVector>(
      context.pool(),
      MAP(toType.keyType(), toType.valueType()),
      input->nulls(),
      rows.end(),
      input->offsets(),
      sizes,
      newMapKeys,
      newMapValues);

  propagateErrorsOrSetNulls(
      setNullInResultAtError(),
      context,
      nestedRows,
      elementToTopLevelRows,
      result,
      oldErrors);

  // Restore original state.
  context.swapErrors(oldErrors);
  return result;
}

VectorPtr CastExprV2::castArray(
    const SelectivityVector& rows,
    const ArrayVector* input,
    exec::EvalCtx& context,
    const ArrayType& fromType,
    const ArrayType& toType) {
  // Cast input array elements to output array elements based on their
  // types using their linear selectivity vector
  auto arrayElements = input->elements();

  auto nestedRows =
      functions::toElementRows(arrayElements->size(), rows, input);
  auto elementToTopLevelRows = functions::getElementToTopLevelRows(
      arrayElements->size(), rows, input, context.pool());

  EvalErrorsPtr oldErrors;
  context.swapErrors(oldErrors);

  VectorPtr newElements;
  {
    ScopedVarSetter holder(&inTopLevel, false);
    apply(
        nestedRows,
        arrayElements,
        context,
        fromType.elementType(),
        toType.elementType(),
        newElements);
  }

  // Returned array vector should be addressable for every element,
  // even those that are not selected.
  BufferPtr sizes = input->sizes();
  if (newElements->isConstantEncoding()) {
    // If the newElements we extends its size since that is cheap.
    newElements->resize(input->elements()->size());
  } else if (newElements->size() < input->elements()->size()) {
    sizes =
        AlignedBuffer::allocate<vector_size_t>(rows.end(), context.pool(), 0);
    auto* inputSizes = input->rawSizes();
    auto* rawSizes = sizes->asMutable<vector_size_t>();
    rows.applyToSelected(
        [&](vector_size_t row) { rawSizes[row] = inputSizes[row]; });
  }

  VectorPtr result = std::make_shared<ArrayVector>(
      context.pool(),
      ARRAY(toType.elementType()),
      input->nulls(),
      rows.end(),
      input->offsets(),
      sizes,
      newElements);

  propagateErrorsOrSetNulls(
      setNullInResultAtError(),
      context,
      nestedRows,
      elementToTopLevelRows,
      result,
      oldErrors);
  // Restore original state.
  context.swapErrors(oldErrors);
  return result;
}

VectorPtr CastExprV2::castRow(
    const SelectivityVector& rows,
    const RowVector* input,
    exec::EvalCtx& context,
    const RowType& fromType,
    const TypePtr& toType) {
  const RowType& toRowType = toType->asRow();
  int numInputChildren = input->children().size();
  int numOutputChildren = toRowType.size();

  // Extract the flag indicating matching of children must be done by
  // name or position
  auto matchByName =
      context.execCtx()->queryCtx()->queryConfig().isMatchStructByName();

  // Cast each row child to its corresponding output child
  std::vector<VectorPtr> newChildren;
  newChildren.reserve(numOutputChildren);

  EvalErrorsPtr oldErrors;
  if (setNullInResultAtError()) {
    // We need to isolate errors that happen during the cast from
    // previous errors since those translate to nulls, unlike
    // exisiting errors.
    context.swapErrors(oldErrors);
  }

  for (auto toChildrenIndex = 0; toChildrenIndex < numOutputChildren;
       toChildrenIndex++) {
    // For each child, find the corresponding column index in the
    // output
    const auto& toFieldName = toRowType.nameOf(toChildrenIndex);
    bool matchNotFound = false;

    // If match is by field name and the input field name is not found
    // in the output row type, do not consider it in the output
    int fromChildrenIndex = -1;
    if (matchByName) {
      if (!fromType.containsChild(toFieldName)) {
        matchNotFound = true;
      } else {
        fromChildrenIndex = fromType.getChildIdx(toFieldName);
        toChildrenIndex = toRowType.getChildIdx(toFieldName);
      }
    } else {
      fromChildrenIndex = toChildrenIndex;
      if (fromChildrenIndex >= numInputChildren) {
        matchNotFound = true;
      }
    }

    // Updating output types and names
    VectorPtr outputChild;
    const auto& toChildType = toRowType.childAt(toChildrenIndex);

    if (matchNotFound) {
      // Create a vector for null for this child
      context.ensureWritable(rows, toChildType, outputChild);
      outputChild->addNulls(rows);
    } else {
      const auto& inputChild = input->children()[fromChildrenIndex];
      if (*toChildType == *inputChild->type()) {
        outputChild = inputChild;
      } else {
        // Apply cast for the child.
        ScopedVarSetter holder(&inTopLevel, false);
        apply(
            rows,
            inputChild,
            context,
            inputChild->type(),
            toChildType,
            outputChild);
      }
    }
    newChildren.emplace_back(std::move(outputChild));
  }

  // Assemble the output row
  VectorPtr result = std::make_shared<RowVector>(
      context.pool(),
      toType,
      input->nulls(),
      rows.end(),
      std::move(newChildren));

  if (setNullInResultAtError()) {
    // Set errors as nulls.
    if (auto errors = context.errors()) {
      rows.applyToSelected([&](auto row) {
        if (errors->hasErrorAt(row)) {
          result->setNull(row, true);
        }
      });
    }
    // Restore original state.
    context.swapErrors(oldErrors);
  }

  return result;
}

template <typename toDecimalType>
VectorPtr CastExprV2::castDecimal(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType) {
  VectorPtr castResult;
  context.ensureWritable(rows, toType, castResult);
  (*castResult).clearNulls(rows);

  // toType is a decimal
  switch (fromType->kind()) {
    case TypeKind::BOOLEAN:
      castIntToDecimalKernel<bool, toDecimalType>(
          rows, input, context, toType, castResult);
      break;
    case TypeKind::TINYINT:
      castIntToDecimalKernel<int8_t, toDecimalType>(
          rows, input, context, toType, castResult);
      break;
    case TypeKind::SMALLINT:
      castIntToDecimalKernel<int16_t, toDecimalType>(
          rows, input, context, toType, castResult);
      break;
    case TypeKind::INTEGER:
      castIntToDecimalKernel<int32_t, toDecimalType>(
          rows, input, context, toType, castResult);
      break;
    case TypeKind::REAL:
      castFloatToDecimalKernel<float, toDecimalType>(
          rows, input, context, toType, castResult);
      break;
    case TypeKind::DOUBLE:
      castFloatToDecimalKernel<double, toDecimalType>(
          rows, input, context, toType, castResult);
      break;
    case TypeKind::BIGINT: {
      if (fromType->isShortDecimal()) {
        castDecimalToDecimalKernel<int64_t, toDecimalType>(
            rows, input, context, fromType, toType, castResult);
        break;
      }
      castIntToDecimalKernel<int64_t, toDecimalType>(
          rows, input, context, toType, castResult);
      break;
    }
    case TypeKind::HUGEINT: {
      if (fromType->isLongDecimal()) {
        castDecimalToDecimalKernel<int128_t, toDecimalType>(
            rows, input, context, fromType, toType, castResult);
        break;
      }
      [[fallthrough]];
    }
    case TypeKind::VARCHAR:
      castVarcharToDecimalKernel<toDecimalType>(
          rows, input, context, toType, castResult);
      break;
    default:
      VELOX_UNSUPPORTED(
          "Cast from {} to {} is not supported",
          fromType->toString(),
          toType->toString());
  }
  return castResult;
}

void CastExprV2::castPeeled(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType,
    VectorPtr& result) {
  auto castFromOperator = getCastOperator(fromType);
  auto castToOperator = getCastOperator(toType);

  // CastRulesRegistry is the source of truth for all custom type cast
  // validation, including container types (e.g., ARRAY<T> → JSON).
  if ((castFromOperator || castToOperator) &&
      !CastRulesRegistry::instance().canCast(fromType, toType)) {
    VELOX_USER_FAIL(
        "Cannot cast {} to {}.", fromType->toString(), toType->toString());
  }

  if (castFromOperator || castToOperator) {
    VELOX_USER_CHECK(
        *fromType != *toType,
        "Attempting to cast from {} to itself.",
        fromType->toString());

    auto applyCustomCast = [&]() {
      if (castToOperator) {
        castToOperator->castTo(input, context, rows, toType, result, hooks_);
      } else {
        castFromOperator->castFrom(input, context, rows, toType, result);
      }
    };

    if (setNullInResultAtError()) {
      // This can be optimized by passing setNullInResultAtError() to
      // castTo and castFrom operations.

      EvalErrorsPtr oldErrors;
      context.swapErrors(oldErrors);

      applyCustomCast();

      if (context.errors()) {
        auto errors = context.errors();
        auto rawNulls = result->mutableRawNulls();

        rows.applyToSelected([&](auto row) {
          if (errors->hasErrorAt(row)) {
            bits::setNull(rawNulls, row, true);
          }
        });
      };
      // Restore original state.
      context.swapErrors(oldErrors);

    } else {
      applyCustomCast();
    }
  } else if (fromType->isDate()) {
    result = castFromDate(rows, input, context, toType);
  } else if (toType->isDate()) {
    result = castToDate(rows, input, context, fromType);
  } else if (fromType->isIntervalDayTime()) {
    result = castFromIntervalDayTime(rows, input, context, toType);
  } else if (toType->isIntervalDayTime()) {
    VELOX_UNSUPPORTED(
        "Cast from {} to {} is not supported",
        fromType->toString(),
        toType->toString());
  } else if (fromType->isTime()) {
    VELOX_DCHECK(fromType->equivalent(*TIME()));
    result = castFromTime(rows, input, context, toType);
  } else if (toType->isTime()) {
    VELOX_DCHECK(toType->equivalent(*TIME()));
    result = castToTime(rows, input, context, fromType);
  } else if (toType->isShortDecimal()) {
    result = castDecimal<int64_t>(rows, input, context, fromType, toType);
  } else if (toType->isLongDecimal()) {
    result = castDecimal<int128_t>(rows, input, context, fromType, toType);
  } else if (fromType->isDecimal()) {
    switch (toType->kind()) {
      case TypeKind::VARCHAR:
        result = VELOX_DYNAMIC_DECIMAL_TYPE_DISPATCH(
            castDecimalToVarchar,
            fromType,
            rows,
            input,
            context,
            fromType);
        break;
      default:
        result = VELOX_DYNAMIC_DECIMAL_TYPE_DISPATCH(
            castDecimalToPrimitive,
            fromType,
            rows,
            input,
            context,
            fromType,
            toType);
    }
  } else if (
      fromType->kind() == TypeKind::TIMESTAMP &&
      (toType->kind() == TypeKind::VARCHAR ||
       toType->kind() == TypeKind::VARBINARY)) {
    VELOX_DCHECK(fromType->equivalent(*TIMESTAMP()));
    result = castTimestampToVarchar(toType, rows, context, input);
  } else if (toType->kind() == TypeKind::VARBINARY) {
    switch (fromType->kind()) {
      case TypeKind::TINYINT:
        result = castIntToBinary<int8_t>(rows, context, input);
        break;
      case TypeKind::SMALLINT:
        result = castIntToBinary<int16_t>(rows, context, input);
        break;
      case TypeKind::INTEGER:
        result = castIntToBinary<int32_t>(rows, context, input);
        break;
      case TypeKind::BIGINT:
        result = castIntToBinary<int64_t>(rows, context, input);
        break;
      default:
        // Handle primitive type conversions.
        castPrimitivesDispatch<TypeKind::VARBINARY>(
            fromType, toType, rows, context, input, result);
        break;
    }
  } else {
    switch (toType->kind()) {
      case TypeKind::MAP:
        result = castMap(
            rows,
            input.asUnchecked<MapVector>(),
            context,
            fromType->asMap(),
            toType->asMap());
        break;
      case TypeKind::ARRAY:
        result = castArray(
            rows,
            input.asUnchecked<ArrayVector>(),
            context,
            fromType->asArray(),
            toType->asArray());
        break;
      case TypeKind::ROW:
        result = castRow(
            rows,
            input.asUnchecked<RowVector>(),
            context,
            fromType->asRow(),
            toType);
        break;
      default: {
        // Handle primitive type conversions.
        VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
            castPrimitivesDispatch,
            toType->kind(),
            fromType,
            toType,
            rows,
            context,
            input,
            result);
      }
    }
  }
}

VectorPtr CastExprV2::castTimestampToVarchar(
    const TypePtr& toType,
    const SelectivityVector& rows,
    exec::EvalCtx& context,
    const BaseVector& input) {
  VectorPtr result;
  context.ensureWritable(rows, toType, result);
  (*result).clearNulls(rows);
  auto flatResult = result->asFlatVector<StringView>();
  const auto simpleInput = input.as<SimpleVector<Timestamp>>();

  const auto& options = hooks_->timestampToStringOptions();
  const uint32_t rowSize = getMaxStringLength(options);

  Buffer* buffer = flatResult->getBufferWithSpace(
      rows.countSelected() * rowSize, true /*exactSize*/);
  char* rawBuffer = buffer->asMutable<char>() + buffer->size();

  applyToSelectedNoThrowLocal(context, rows, result, [&](vector_size_t row) {
    // Adjust input timestamp according the session timezone.
    Timestamp inputValue(simpleInput->valueAt(row));
    if (options.timeZone) {
      inputValue.toTimezone(*(options.timeZone));
    }
    const auto stringView =
        Timestamp::tsToStringView(inputValue, options, rawBuffer);
    flatResult->setNoCopy(row, stringView);
    // The result of both Presto and Spark contains more than 12
    // digits even when 'zeroPaddingYear' is disabled.
    VELOX_DCHECK(!stringView.isInline());
    rawBuffer += stringView.size();
  });

  // Update the exact buffer size.
  buffer->setSize(rawBuffer - buffer->asMutable<char>());
  return result;
}

template <typename TInput>
VectorPtr CastExprV2::castIntToBinary(
    const SelectivityVector& rows,
    exec::EvalCtx& context,
    const BaseVector& input) {
  auto result = BaseVector::create(VARBINARY(), rows.end(), context.pool());
  const auto flatResult = result->asFlatVector<StringView>();
  const auto simpleInput = input.as<SimpleVector<TInput>>();

  // The created string view is always inlined for int types.
  char inlined[sizeof(TInput)];
  applyToSelectedNoThrowLocal(context, rows, result, [&](vector_size_t row) {
    TInput input = simpleInput->valueAt(row);
    if constexpr (std::is_same_v<TInput, int8_t>) {
      inlined[0] = static_cast<char>(input & 0xFF);
    } else {
      for (int i = sizeof(TInput) - 1; i >= 0; --i) {
        inlined[i] = static_cast<char>(input & 0xFF);
        input >>= 8;
      }
    }
    const auto stringView = StringView(inlined, sizeof(TInput));
    flatResult->setNoCopy(row, stringView);
  });

  return result;
}

void CastExprV2::apply(
    const SelectivityVector& rows,
    const VectorPtr& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType,
    VectorPtr& result) {
  LocalSelectivityVector remainingRows(context, rows);

  context.deselectErrors(*remainingRows);

  LocalDecodedVector decoded(context, *input, *remainingRows);
  auto* rawNulls = decoded->nulls(remainingRows.get());

  if (rawNulls) {
    remainingRows->deselectNulls(
        rawNulls, remainingRows->begin(), remainingRows->end());
  }

  VectorPtr localResult;
  if (!remainingRows->hasSelections()) {
    localResult =
        BaseVector::createNullConstant(toType, rows.end(), context.pool());
  } else if (decoded->isIdentityMapping()) {
    castPeeled(
        *remainingRows,
        *decoded->base(),
        context,
        fromType,
        toType,
        localResult);
  } else {
    withContextSaver([&](ContextSaver& saver) {
      LocalSelectivityVector newRowsHolder(*context.execCtx());

      LocalDecodedVector localDecoded(context);
      std::vector<VectorPtr> peeledVectors;
      auto peeledEncoding = PeeledEncoding::peel(
          {input}, *remainingRows, localDecoded, true, peeledVectors);
      VELOX_CHECK_EQ(peeledVectors.size(), 1);
      if (peeledVectors[0]->isLazy()) {
        peeledVectors[0] =
            peeledVectors[0]->as<LazyVector>()->loadedVectorShared();
      }
      auto newRows =
          peeledEncoding->translateToInnerRows(*remainingRows, newRowsHolder);
      // Save context and set the peel.
      context.saveAndReset(saver, *remainingRows);
      context.setPeeledEncoding(peeledEncoding);
      castPeeled(
          *newRows, *peeledVectors[0], context, fromType, toType, localResult);

      localResult = context.getPeeledEncoding()->wrap(
          toType, context.pool(), localResult, *remainingRows);
    });
  }
  context.moveOrCopyResult(localResult, *remainingRows, result);
  context.releaseVector(localResult);

  // If there are nulls or rows that encountered errors in the input,
  // add nulls to the result at the same rows.
  VELOX_CHECK_NOT_NULL(result);
  if (rawNulls || context.errors()) {
    EvalCtx::addNulls(
        rows, remainingRows->asRange().bits(), context, toType, result);
  }
}

void CastExprV2::evalSpecialForm(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  VectorPtr input;
  inputs_[0]->eval(rows, context, input);
  auto fromType = inputs_[0]->type();
  auto toType = std::const_pointer_cast<const Type>(type_);

  inTopLevel = true;
  if (isTryCast()) {
    ScopedVarSetter holder{context.mutableThrowOnError(), false};
    ScopedVarSetter captureErrorDetails(
        context.mutableCaptureErrorDetails(), false);

    ScopedThreadSkipErrorDetails skipErrorDetails(true);

    apply(rows, input, context, fromType, toType, result);
  } else {
    apply(rows, input, context, fromType, toType, result);
  }
  // Return 'input' back to the vector pool in 'context' so it can be
  // reused.
  context.releaseVector(input);
}

std::string CastExprV2::toString(bool recursive) const {
  std::stringstream out;
  out << name() << "(";
  if (recursive) {
    appendInputs(out);
  } else {
    out << inputs_[0]->toString(false);
  }
  out << " as " << type_->toString() << ")";
  return out.str();
}

std::string CastExprV2::toSql(std::vector<VectorPtr>* complexConstants) const {
  std::stringstream out;
  out << name() << "(";
  appendInputsSql(out, complexConstants);
  out << " as ";
  toTypeSql(type_, out);
  out << ")";
  return out.str();
}

CastOperatorPtr CastExprV2::getCastOperator(const TypePtr& type) {
  const auto* key = type->name();

  auto it = castOperators_.find(key);
  if (it != castOperators_.end()) {
    return it->second;
  }

  auto castOperator = getCustomTypeCastOperator(key);
  if (castOperator == nullptr) {
    return nullptr;
  }

  castOperators_.emplace(key, castOperator);
  return castOperator;
}

// V1's CastCallToSpecialForm / TryCastCallToSpecialForm registration
// helpers are intentionally not duplicated here -- those remain in
// CastExpr.cpp and feed into the shared compiler registry.  When V2
// switches to native CastExprV2, the wiring will be added in a
// dedicated commit.

// static
void CastExprV2::setHooksFactory(HooksFactory factory) {
  VELOX_CHECK(factory, "CastExprV2 hooks factory must be non-null");
  globalHooksFactory() = std::move(factory);
}

// static
const CastExprV2::HooksFactory& CastExprV2::hooksFactory() {
  return globalHooksFactory();
}

} // namespace facebook::velox::exec
