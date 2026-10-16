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

#include "velox/expression/ExprToSubfieldFilter.h"

#include <span>

#include "velox/expression/Expr.h"
#include "velox/type/StringFunctionFilter.h"

using namespace facebook::velox;

namespace facebook::velox::exec {

namespace {

VectorPtr toConstant(
    const core::TypedExprPtr& expr,
    core::ExpressionEvaluator* evaluator) {
  auto exprSet = evaluator->compile(expr);
  if (!exprSet->exprs()[0]->isConstantExpr()) {
    return nullptr;
  }
  RowVector input(
      evaluator->pool(), ROW({}, {}), nullptr, 1, std::vector<VectorPtr>{});
  SelectivityVector rows(1);
  VectorPtr result;
  try {
    evaluator->evaluate(exprSet.get(), rows, input, result);
  } catch (const VeloxUserError&) {
    return nullptr;
  }
  return result;
}

template <typename T>
T singleValue(const VectorPtr& vector) {
  auto simpleVector = vector->as<SimpleVector<T>>();
  VELOX_CHECK_NOT_NULL(simpleVector);
  return simpleVector->valueAt(0);
}

// True if 'kind' is an IN-list element type for which a subfield filter is
// built (string BytesValues or widened-int BigintValues).
bool isSupportedInElementKind(TypeKind kind) {
  return kind == TypeKind::VARCHAR || kind == TypeKind::TINYINT ||
      kind == TypeKind::SMALLINT || kind == TypeKind::INTEGER ||
      kind == TypeKind::BIGINT;
}

enum class ComparisonKind { kEq, kNeq, kLt, kLte, kGt, kGte };

std::optional<ComparisonKind> comparisonKind(std::string_view name) {
  if (name == "eq") {
    return ComparisonKind::kEq;
  }
  if (name == "neq") {
    return ComparisonKind::kNeq;
  }
  if (name == "lt") {
    return ComparisonKind::kLt;
  }
  if (name == "lte") {
    return ComparisonKind::kLte;
  }
  if (name == "gt") {
    return ComparisonKind::kGt;
  }
  if (name == "gte") {
    return ComparisonKind::kGte;
  }
  return std::nullopt;
}

ComparisonKind negateComparison(ComparisonKind comparison) {
  switch (comparison) {
    case ComparisonKind::kEq:
      return ComparisonKind::kNeq;
    case ComparisonKind::kNeq:
      return ComparisonKind::kEq;
    case ComparisonKind::kLt:
      return ComparisonKind::kGte;
    case ComparisonKind::kLte:
      return ComparisonKind::kGt;
    case ComparisonKind::kGt:
      return ComparisonKind::kLte;
    case ComparisonKind::kGte:
      return ComparisonKind::kLt;
  }
  VELOX_UNREACHABLE();
}

ComparisonKind flipComparison(ComparisonKind comparison) {
  switch (comparison) {
    case ComparisonKind::kEq:
    case ComparisonKind::kNeq:
      return comparison;
    case ComparisonKind::kLt:
      return ComparisonKind::kGt;
    case ComparisonKind::kLte:
      return ComparisonKind::kGte;
    case ComparisonKind::kGt:
      return ComparisonKind::kLt;
    case ComparisonKind::kGte:
      return ComparisonKind::kLte;
  }
  VELOX_UNREACHABLE();
}

bool isPrimitiveIntegralType(const Type* type) {
  static const Type* const kTinyint = TINYINT().get();
  static const Type* const kSmallint = SMALLINT().get();
  static const Type* const kInteger = INTEGER().get();
  static const Type* const kBigint = BIGINT().get();
  return type == kTinyint || type == kSmallint || type == kInteger ||
      type == kBigint;
}

std::optional<int64_t> integralConstant(
    const VectorPtr& value,
    const Type* expectedType) {
  if (expectedType == nullptr) {
    return std::nullopt;
  }
  if (value->type().get() != expectedType) {
    return std::nullopt;
  }
  const auto expectedKind = expectedType->kind();
  if (expectedKind == TypeKind::TINYINT) {
    return singleValue<TypeTraits<TypeKind::TINYINT>::NativeType>(value);
  }
  if (expectedKind == TypeKind::SMALLINT) {
    return singleValue<TypeTraits<TypeKind::SMALLINT>::NativeType>(value);
  }
  if (expectedKind == TypeKind::INTEGER) {
    return singleValue<TypeTraits<TypeKind::INTEGER>::NativeType>(value);
  }
  VELOX_CHECK_EQ(expectedKind, TypeKind::BIGINT);
  return singleValue<TypeTraits<TypeKind::BIGINT>::NativeType>(value);
}

std::unique_ptr<common::Filter> makeIntegralComparisonFilter(
    ComparisonKind comparison,
    int64_t value,
    const Type* sourceType) {
  VELOX_CHECK_NOT_NULL(sourceType);
  const auto numBits = sourceType->cppSizeInBytes() * 8;
  const auto max = numBits == 64 ? std::numeric_limits<int64_t>::max()
                                 : (int64_t{1} << (numBits - 1)) - 1;
  const auto min = -max - 1;
  // The preimage of a comparison with an out-of-range constant contains
  // either every non-null source value or none of them.
  if (value < min) {
    switch (comparison) {
      case ComparisonKind::kNeq:
      case ComparisonKind::kGt:
      case ComparisonKind::kGte:
        return isNotNull();
      case ComparisonKind::kEq:
      case ComparisonKind::kLt:
      case ComparisonKind::kLte:
        return std::make_unique<common::AlwaysFalse>();
    }
  }
  if (value > max) {
    switch (comparison) {
      case ComparisonKind::kNeq:
      case ComparisonKind::kLt:
      case ComparisonKind::kLte:
        return isNotNull();
      case ComparisonKind::kEq:
      case ComparisonKind::kGt:
      case ComparisonKind::kGte:
        return std::make_unique<common::AlwaysFalse>();
    }
  }

  switch (comparison) {
    case ComparisonKind::kEq:
      return equal(value);
    case ComparisonKind::kNeq:
      return bigintOr(lessThan(value), greaterThan(value));
    case ComparisonKind::kLt:
      return lessThan(value);
    case ComparisonKind::kLte:
      return lessThanOrEqual(value);
    case ComparisonKind::kGt:
      return greaterThan(value);
    case ComparisonKind::kGte:
      return greaterThanOrEqual(value);
  }
  VELOX_UNREACHABLE();
}

struct IntegralWideningCast {
  const core::ITypedExpr* sourceExpr;
  const Type* sourceType;
  const Type* targetType;
};

std::optional<IntegralWideningCast> unwrapIntegralWideningCast(
    const core::ITypedExpr* expr) {
  if (expr == nullptr) {
    return std::nullopt;
  }
  const auto* targetType = expr->type().get();
  if (targetType == nullptr || !isPrimitiveIntegralType(targetType)) {
    return std::nullopt;
  }
  const core::ITypedExpr* sourceExpr = expr;
  const Type* sourceType = targetType;
  bool foundCast = false;
  // A widening integral cast chain is lossless and order-preserving, so the
  // comparison can be expressed exactly in the source column domain.
  while (const auto* cast =
             dynamic_cast<const core::CastTypedExpr*>(sourceExpr)) {
    const auto* castSourceType = cast->inputs()[0]->type().get();
    const auto* castTargetType = cast->type().get();
    if (cast->isTryCast() || castSourceType == nullptr ||
        castTargetType == nullptr || !isPrimitiveIntegralType(castSourceType) ||
        !isPrimitiveIntegralType(castTargetType) ||
        castSourceType->cppSizeInBytes() >= castTargetType->cppSizeInBytes()) {
      return std::nullopt;
    }
    foundCast = true;
    sourceExpr = cast->inputs()[0].get();
    sourceType = castSourceType;
  }
  if (!foundCast || sourceExpr == nullptr) {
    return std::nullopt;
  }
  return IntegralWideningCast{sourceExpr, sourceType, targetType};
}

std::unique_ptr<common::Filter> makeIntegralWideningCastFilter(
    const IntegralWideningCast& cast,
    const core::TypedExprPtr& valueExpr,
    ComparisonKind comparison,
    core::ExpressionEvaluator* evaluator) {
  auto constant = toConstant(valueExpr, evaluator);
  if (constant == nullptr) {
    return nullptr;
  }
  if (constant->isNullAt(0)) {
    return std::make_unique<common::AlwaysFalse>();
  }
  auto value = integralConstant(constant, cast.targetType);
  if (!value.has_value()) {
    return nullptr;
  }
  return makeIntegralComparisonFilter(comparison, *value, cast.sourceType);
}

bool isBigintRange(const std::unique_ptr<common::Filter>& filter) {
  return filter->is(common::FilterKind::kBigintRange);
}

bool isBigintMultiRange(const std::unique_ptr<common::Filter>& filter) {
  return filter->is(common::FilterKind::kBigintMultiRange);
}

bool isNegatedBigintRange(const std::unique_ptr<common::Filter>& filter) {
  return filter->is(common::FilterKind::kNegatedBigintRange);
}

bool isBytesValues(const std::unique_ptr<common::Filter>& filter) {
  return filter->is(common::FilterKind::kBytesValues);
}

bool isBytesRange(const std::unique_ptr<common::Filter>& filter) {
  return filter->is(common::FilterKind::kBytesRange);
}

bool isSingleValueBytesRange(const std::unique_ptr<common::Filter>& filter) {
  if (!isBytesRange(filter)) {
    return false;
  }
  const auto* bytesRange = filter->as<common::BytesRange>();
  return bytesRange->isSingleValue();
}

template <typename T, typename U>
std::unique_ptr<T> asUniquePtr(std::unique_ptr<U> ptr) {
  return std::unique_ptr<T>(static_cast<T*>(ptr.release()));
}

std::unique_ptr<common::BigintRange> asBigintRange(
    std::unique_ptr<common::Filter>& ptr) {
  return asUniquePtr<common::BigintRange>(std::move(ptr));
}

// Builds an IN (or NOT IN) filter from the collected non-NULL 'values'.
// 'hasNull' records whether the list contained a NULL. An IN over only NULLs,
// and a NOT IN over any list containing a NULL, can never pass (SQL
// three-valued logic: `a [NOT] IN (..., NULL)` is NULL), so both become
// AlwaysFalse. Works for both int64 and string value lists via the overloaded
// in()/notIn() helpers.
template <typename T>
std::unique_ptr<common::Filter>
finishInFilter(std::vector<T> values, bool hasNull, bool negated) {
  if (values.empty()) {
    return std::make_unique<common::AlwaysFalse>();
  }
  if (negated) {
    if (hasNull) {
      return std::make_unique<common::AlwaysFalse>();
    }
    return notIn(values);
  }
  return in(values);
}

template <typename T>
std::unique_ptr<common::Filter> toInt64In(
    const VectorPtr& vector,
    vector_size_t start,
    vector_size_t size,
    bool negated) {
  auto ints = vector->as<SimpleVector<T>>();
  std::vector<int64_t> values;
  values.reserve(size);
  bool hasNull = false;
  if (!ints->mayHaveNulls()) {
    for (auto i = 0; i < size; i++) {
      values.push_back(ints->valueAt(start + i));
    }
  } else {
    for (auto i = 0; i < size; i++) {
      if (!ints->isNullAt(start + i)) {
        values.push_back(ints->valueAt(start + i));
      } else if (!hasNull) {
        hasNull = true;
      }
    }
  }

  return finishInFilter(std::move(values), hasNull, negated);
}

// Builds an int64 IN filter from a list of single-row constant vectors (the
// varargs `in(col, a, b, c, ...)` elements), widening from the integral element
// type 'T'. Counterpart to the range-based toInt64In above.
template <typename T>
std::unique_ptr<common::Filter> toInt64In(
    const std::vector<VectorPtr>& constants,
    bool negated) {
  std::vector<int64_t> values;
  values.reserve(constants.size());
  bool hasNull = false;
  for (const auto& constant : constants) {
    if (constant->isNullAt(0)) {
      hasNull = true;
    } else {
      values.push_back(singleValue<T>(constant));
    }
  }
  return finishInFilter(std::move(values), hasNull, negated);
}

// Builds a string IN filter from the [start, start + size) slice of 'vector'
// (the `in(col, <ARRAY(VARCHAR) constant>)` elements).
std::unique_ptr<common::Filter> toVarcharIn(
    const VectorPtr& vector,
    vector_size_t start,
    vector_size_t size,
    bool negated) {
  auto strings = vector->as<SimpleVector<StringView>>();
  std::vector<std::string> values;
  values.reserve(size);
  bool hasNull = false;
  for (auto i = 0; i < size; i++) {
    if (strings->isNullAt(start + i)) {
      hasNull = true;
    } else {
      values.push_back(std::string(strings->valueAt(start + i)));
    }
  }
  return finishInFilter(std::move(values), hasNull, negated);
}

// Builds a string IN filter from a list of single-row constant vectors (the
// varargs `in(col, a, b, c, ...)` elements). Counterpart to the slice-based
// toVarcharIn above.
std::unique_ptr<common::Filter> toVarcharIn(
    const std::vector<VectorPtr>& constants,
    bool negated) {
  std::vector<std::string> values;
  values.reserve(constants.size());
  bool hasNull = false;
  for (const auto& constant : constants) {
    if (constant->isNullAt(0)) {
      hasNull = true;
    } else {
      values.push_back(std::string(singleValue<StringView>(constant)));
    }
  }
  return finishInFilter(std::move(values), hasNull, negated);
}

} // namespace

namespace {
std::unique_ptr<common::Filter> bigintOrImpl(
    std::vector<std::unique_ptr<common::Filter>> inputs,
    bool nullAllowed) {
  std::vector<std::unique_ptr<common::BigintRange>> ranges;
  for (auto& input : inputs) {
    if (input->kind() == common::FilterKind::kIsNull) {
      nullAllowed = true;
    } else if (input->kind() != common::FilterKind::kAlwaysFalse) {
      VELOX_CHECK(
          isBigintRange(input),
          "Expected BigintRange filter, got: {}",
          input->toString());
      ranges.emplace_back(asBigintRange(input));
    }
  }
  if (ranges.empty()) {
    if (nullAllowed) {
      return std::make_unique<common::IsNull>();
    }
    return std::make_unique<common::AlwaysFalse>();
  }
  if (ranges.size() == 1) {
    if (nullAllowed) {
      return std::make_unique<common::BigintRange>(
          ranges[0]->lower(), ranges[0]->upper(), true);
    }
    return std::move(ranges[0]);
  }
  return std::make_unique<common::BigintMultiRange>(
      std::move(ranges), nullAllowed);
}
} // namespace

std::unique_ptr<common::Filter> bigintOr(
    std::unique_ptr<common::Filter> a,
    std::unique_ptr<common::Filter> b,
    bool nullAllowed) {
  std::vector<std::unique_ptr<common::Filter>> inputs;
  inputs.emplace_back(std::move(a));
  inputs.emplace_back(std::move(b));
  return bigintOrImpl(std::move(inputs), nullAllowed);
}

std::unique_ptr<common::Filter> bigintOr(
    std::unique_ptr<common::Filter> a,
    std::unique_ptr<common::Filter> b,
    std::unique_ptr<common::Filter> c,
    bool nullAllowed) {
  std::vector<std::unique_ptr<common::Filter>> inputs;
  inputs.emplace_back(std::move(a));
  inputs.emplace_back(std::move(b));
  inputs.emplace_back(std::move(c));
  return bigintOrImpl(std::move(inputs), nullAllowed);
}

std::shared_ptr<ExprToSubfieldFilterParser>
    ExprToSubfieldFilterParser::parser_ =
        std::make_shared<PrestoExprToSubfieldFilterParser>();

std::optional<std::pair<common::Subfield, std::unique_ptr<common::Filter>>>
ExprToSubfieldFilterParser::leafToSubfieldFilter(
    const core::ITypedExpr& expr,
    core::ExpressionEvaluator* evaluator,
    bool negated) {
  common::Subfield subfield;
  if (expr.type()->isBoolean() && toSubfield(&expr, subfield)) {
    return std::make_pair(std::move(subfield), boolEqual(!negated));
  }

  if (auto* call = dynamic_cast<const core::CallTypedExpr*>(&expr)) {
    return leafCallToSubfieldFilter(*call, evaluator, negated);
  }
  return std::nullopt;
}

// static
bool ExprToSubfieldFilterParser::toSubfield(
    const core::ITypedExpr* field,
    common::Subfield& subfield) {
  std::vector<std::unique_ptr<common::Subfield::PathElement>> path;
  for (auto* current = field;;) {
    if (auto* fieldAccess =
            dynamic_cast<const core::FieldAccessTypedExpr*>(current)) {
      path.push_back(
          std::make_unique<common::Subfield::NestedField>(fieldAccess->name()));
    } else if (
        auto* dereference =
            dynamic_cast<const core::DereferenceTypedExpr*>(current)) {
      const auto& name = dereference->name();
      // When the field name is empty string, it typically means that the
      // field name was not set in the parent type.
      if (name.empty()) {
        return false;
      }
      path.push_back(std::make_unique<common::Subfield::NestedField>(name));
    } else if (dynamic_cast<const core::InputTypedExpr*>(current) == nullptr) {
      return false;
    } else {
      break;
    }

    if (current->inputs().empty()) {
      break;
    }
    if (current->inputs().size() != 1) {
      return false;
    }
    current = current->inputs()[0].get();
    if (current == nullptr) {
      return false;
    }
  }
  std::reverse(path.begin(), path.end());
  subfield = common::Subfield(std::move(path));
  return true;
}

// static
std::unique_ptr<common::Filter> ExprToSubfieldFilterParser::makeNotEqualFilter(
    const core::TypedExprPtr& valueExpr,
    core::ExpressionEvaluator* evaluator) {
  auto value = toConstant(valueExpr, evaluator);
  if (!value) {
    return nullptr;
  }
  if (value->isNullAt(0)) {
    return std::make_unique<common::AlwaysFalse>();
  }

  auto lessThanFilter = makeLessThanFilter(valueExpr, evaluator);
  if (!lessThanFilter) {
    return nullptr;
  }

  auto greaterThanFilter = makeGreaterThanFilter(valueExpr, evaluator);
  if (!greaterThanFilter) {
    return nullptr;
  }

  const auto typeKind = value->typeKind();

  if (typeKind == TypeKind::TINYINT || typeKind == TypeKind::SMALLINT ||
      typeKind == TypeKind::INTEGER || typeKind == TypeKind::BIGINT) {
    if (lessThanFilter->kind() == common::FilterKind::kAlwaysFalse) {
      return greaterThanFilter;
    }
    if (greaterThanFilter->kind() == common::FilterKind::kAlwaysFalse) {
      return lessThanFilter;
    }
    VELOX_CHECK(isBigintRange(lessThanFilter));
    VELOX_CHECK(isBigintRange(greaterThanFilter));
    const auto excluded =
        lessThanFilter->as<common::BigintRange>()->upper() + 1;
    return std::make_unique<common::NegatedBigintRange>(
        excluded, excluded, false);
  }

  if (typeKind == TypeKind::HUGEINT) {
    VELOX_NYI();
  } else {
    if (lessThanFilter->kind() == common::FilterKind::kAlwaysFalse ||
        lessThanFilter->kind() == common::FilterKind::kIsNull) {
      return greaterThanFilter;
    }
    if (greaterThanFilter->kind() == common::FilterKind::kAlwaysFalse ||
        greaterThanFilter->kind() == common::FilterKind::kIsNull) {
      return lessThanFilter;
    }
    std::vector<std::unique_ptr<common::Filter>> filters;
    filters.emplace_back(std::move(lessThanFilter));
    filters.emplace_back(std::move(greaterThanFilter));

    return std::make_unique<common::MultiRange>(std::move(filters), false);
  }
}

// static
std::unique_ptr<common::Filter> ExprToSubfieldFilterParser::makeEqualFilter(
    const core::TypedExprPtr& valueExpr,
    core::ExpressionEvaluator* evaluator) {
  auto value = toConstant(valueExpr, evaluator);
  if (!value) {
    return nullptr;
  }
  if (value->isNullAt(0)) {
    return std::make_unique<common::AlwaysFalse>();
  }
  switch (value->typeKind()) {
    case TypeKind::BOOLEAN:
      return boolEqual(singleValue<bool>(value));
    case TypeKind::TINYINT:
      return equal(singleValue<int8_t>(value));
    case TypeKind::SMALLINT:
      return equal(singleValue<int16_t>(value));
    case TypeKind::INTEGER:
      return equal(singleValue<int32_t>(value));
    case TypeKind::BIGINT:
      return equal(singleValue<int64_t>(value));
    case TypeKind::HUGEINT:
      return equalHugeint(singleValue<int128_t>(value));
    case TypeKind::VARCHAR:
      return equal(std::string(singleValue<StringView>(value)));
    case TypeKind::TIMESTAMP:
      return equal(singleValue<Timestamp>(value));
    default:
      return nullptr;
  }
}

// static
std::unique_ptr<common::Filter>
ExprToSubfieldFilterParser::makeGreaterThanFilter(
    const core::TypedExprPtr& lowerExpr,
    core::ExpressionEvaluator* evaluator) {
  auto lower = toConstant(lowerExpr, evaluator);
  if (!lower) {
    return nullptr;
  }
  if (lower->isNullAt(0)) {
    return std::make_unique<common::AlwaysFalse>();
  }
  switch (lower->typeKind()) {
    case TypeKind::TINYINT:
      return greaterThan(singleValue<int8_t>(lower));
    case TypeKind::SMALLINT:
      return greaterThan(singleValue<int16_t>(lower));
    case TypeKind::INTEGER:
      return greaterThan(singleValue<int32_t>(lower));
    case TypeKind::BIGINT:
      return greaterThan(singleValue<int64_t>(lower));
    case TypeKind::HUGEINT:
      return greaterThanHugeint(singleValue<int128_t>(lower));
    case TypeKind::DOUBLE:
      return greaterThanDouble(singleValue<double>(lower));
    case TypeKind::REAL:
      return greaterThanFloat(singleValue<float>(lower));
    case TypeKind::VARCHAR:
      return greaterThan(std::string(singleValue<StringView>(lower)));
    case TypeKind::TIMESTAMP:
      return greaterThan(singleValue<Timestamp>(lower));
    default:
      return nullptr;
  }
}

// static
std::unique_ptr<common::Filter> ExprToSubfieldFilterParser::makeLessThanFilter(
    const core::TypedExprPtr& upperExpr,
    core::ExpressionEvaluator* evaluator) {
  auto upper = toConstant(upperExpr, evaluator);
  if (!upper) {
    return nullptr;
  }
  if (upper->isNullAt(0)) {
    return std::make_unique<common::AlwaysFalse>();
  }
  switch (upper->typeKind()) {
    case TypeKind::TINYINT:
      return lessThan(singleValue<int8_t>(upper));
    case TypeKind::SMALLINT:
      return lessThan(singleValue<int16_t>(upper));
    case TypeKind::INTEGER:
      return lessThan(singleValue<int32_t>(upper));
    case TypeKind::BIGINT:
      return lessThan(singleValue<int64_t>(upper));
    case TypeKind::HUGEINT:
      return lessThanHugeint(singleValue<int128_t>(upper));
    case TypeKind::DOUBLE:
      return lessThanDouble(singleValue<double>(upper));
    case TypeKind::REAL:
      return lessThanFloat(singleValue<float>(upper));
    case TypeKind::VARCHAR:
      return lessThan(std::string(singleValue<StringView>(upper)));
    case TypeKind::TIMESTAMP:
      return lessThan(singleValue<Timestamp>(upper));
    default:
      return nullptr;
  }
}

// static
std::unique_ptr<common::Filter>
ExprToSubfieldFilterParser::makeLessThanOrEqualFilter(
    const core::TypedExprPtr& upperExpr,
    core::ExpressionEvaluator* evaluator) {
  auto upper = toConstant(upperExpr, evaluator);
  if (!upper) {
    return nullptr;
  }
  if (upper->isNullAt(0)) {
    return std::make_unique<common::AlwaysFalse>();
  }
  switch (upper->typeKind()) {
    case TypeKind::TINYINT:
      return lessThanOrEqual(singleValue<int8_t>(upper));
    case TypeKind::SMALLINT:
      return lessThanOrEqual(singleValue<int16_t>(upper));
    case TypeKind::INTEGER:
      return lessThanOrEqual(singleValue<int32_t>(upper));
    case TypeKind::BIGINT:
      return lessThanOrEqual(singleValue<int64_t>(upper));
    case TypeKind::HUGEINT:
      return lessThanOrEqualHugeint(singleValue<int128_t>(upper));
    case TypeKind::DOUBLE:
      return lessThanOrEqualDouble(singleValue<double>(upper));
    case TypeKind::REAL:
      return lessThanOrEqualFloat(singleValue<float>(upper));
    case TypeKind::VARCHAR:
      return lessThanOrEqual(std::string(singleValue<StringView>(upper)));
    case TypeKind::TIMESTAMP:
      return lessThanOrEqual(singleValue<Timestamp>(upper));
    default:
      return nullptr;
  }
}

// static
std::unique_ptr<common::Filter>
ExprToSubfieldFilterParser::makeGreaterThanOrEqualFilter(
    const core::TypedExprPtr& lowerExpr,
    core::ExpressionEvaluator* evaluator) {
  auto lower = toConstant(lowerExpr, evaluator);
  if (!lower) {
    return nullptr;
  }
  if (lower->isNullAt(0)) {
    return std::make_unique<common::AlwaysFalse>();
  }
  switch (lower->typeKind()) {
    case TypeKind::TINYINT:
      return greaterThanOrEqual(singleValue<int8_t>(lower));
    case TypeKind::SMALLINT:
      return greaterThanOrEqual(singleValue<int16_t>(lower));
    case TypeKind::INTEGER:
      return greaterThanOrEqual(singleValue<int32_t>(lower));
    case TypeKind::BIGINT:
      return greaterThanOrEqual(singleValue<int64_t>(lower));
    case TypeKind::HUGEINT:
      return greaterThanOrEqualHugeint(singleValue<int128_t>(lower));
    case TypeKind::DOUBLE:
      return greaterThanOrEqualDouble(singleValue<double>(lower));
    case TypeKind::REAL:
      return greaterThanOrEqualFloat(singleValue<float>(lower));
    case TypeKind::VARCHAR:
      return greaterThanOrEqual(std::string(singleValue<StringView>(lower)));
    case TypeKind::TIMESTAMP:
      return greaterThanOrEqual(singleValue<Timestamp>(lower));
    default:
      return nullptr;
  }
}

// static
std::unique_ptr<common::Filter> ExprToSubfieldFilterParser::makeInFilter(
    const core::TypedExprPtr& expr,
    core::ExpressionEvaluator* evaluator,
    bool negated) {
  auto vector = toConstant(expr, evaluator);
  if (!vector) {
    return nullptr;
  }
  VELOX_CHECK(vector->type()->isArray());

  auto arrayVector = vector->valueVector()->as<ArrayVector>();
  auto index = vector->as<ConstantVector<ComplexType>>()->index();
  auto offset = arrayVector->offsetAt(index);
  auto size = arrayVector->sizeAt(index);
  auto elements = arrayVector->elements();

  auto elementType = arrayVector->type()->asArray().elementType();
  switch (elementType->kind()) {
    case TypeKind::TINYINT:
      return toInt64In<int8_t>(elements, offset, size, negated);
    case TypeKind::SMALLINT:
      return toInt64In<int16_t>(elements, offset, size, negated);
    case TypeKind::INTEGER:
      return toInt64In<int32_t>(elements, offset, size, negated);
    case TypeKind::BIGINT:
      return toInt64In<int64_t>(elements, offset, size, negated);
    case TypeKind::VARCHAR:
      return toVarcharIn(elements, offset, size, negated);
    default:
      VELOX_UNREACHABLE();
  }
}

// static
std::unique_ptr<common::Filter> ExprToSubfieldFilterParser::makeVarargsInFilter(
    std::span<const core::TypedExprPtr> valueExprs,
    core::ExpressionEvaluator* evaluator,
    bool negated) {
  VELOX_CHECK(!valueExprs.empty());
  const auto kind = valueExprs[0]->type()->kind();

  std::vector<VectorPtr> constants;
  constants.reserve(valueExprs.size());
  for (const auto& expr : valueExprs) {
    auto constant = toConstant(expr, evaluator);
    if (!constant) {
      return nullptr;
    }
    constants.push_back(std::move(constant));
  }

  switch (kind) {
    case TypeKind::TINYINT:
      return toInt64In<int8_t>(constants, negated);
    case TypeKind::SMALLINT:
      return toInt64In<int16_t>(constants, negated);
    case TypeKind::INTEGER:
      return toInt64In<int32_t>(constants, negated);
    case TypeKind::BIGINT:
      return toInt64In<int64_t>(constants, negated);
    case TypeKind::VARCHAR:
      return toVarcharIn(constants, negated);
    default:
      VELOX_UNREACHABLE();
  }
}

// static
std::unique_ptr<common::Filter> ExprToSubfieldFilterParser::makeBetweenFilter(
    const core::TypedExprPtr& lowerExpr,
    const core::TypedExprPtr& upperExpr,
    core::ExpressionEvaluator* evaluator,
    bool negated) {
  auto lower = toConstant(lowerExpr, evaluator);
  if (!lower) {
    return nullptr;
  }
  auto upper = toConstant(upperExpr, evaluator);
  if (!upper) {
    return nullptr;
  }
  switch (lower->typeKind()) {
    case TypeKind::INTEGER:
      if (negated) {
        return notBetween(
            singleValue<int32_t>(lower), singleValue<int32_t>(upper));
      }
      return between(singleValue<int32_t>(lower), singleValue<int32_t>(upper));
    case TypeKind::BIGINT:
      if (negated) {
        return notBetween(
            singleValue<int64_t>(lower), singleValue<int64_t>(upper));
      }
      return between(singleValue<int64_t>(lower), singleValue<int64_t>(upper));
    case TypeKind::DOUBLE:
      return negated
          ? nullptr
          : betweenDouble(
                singleValue<double>(lower), singleValue<double>(upper));
    case TypeKind::REAL:
      return negated
          ? nullptr
          : betweenFloat(singleValue<float>(lower), singleValue<float>(upper));
    case TypeKind::VARCHAR:
      if (negated) {
        return notBetween(
            std::string(singleValue<StringView>(lower)),
            std::string(singleValue<StringView>(upper)));
      }
      return between(
          std::string(singleValue<StringView>(lower)),
          std::string(singleValue<StringView>(upper)));
    case TypeKind::TIMESTAMP:
      return negated
          ? nullptr
          : between(
                singleValue<Timestamp>(lower), singleValue<Timestamp>(upper));
    default:
      return nullptr;
  }
}

namespace {

bool isNullAllowed(
    const std::vector<std::unique_ptr<common::Filter>>& disjuncts) {
  return std::any_of(
      disjuncts.begin(), disjuncts.end(), [](const auto& filter) {
        return filter->nullAllowed();
      });
}

// Combines overlapping ranges into one using OR semantic. Returns nullptr if
// ranges do not overlap. Ignores nullAllowed flag.
// @pre a.lower() <= b.lower()
std::unique_ptr<common::BigintRange> tryMergeOverlappingRanges(
    const common::BigintRange& a,
    const common::BigintRange& b,
    bool& alwaysTrue) {
  static constexpr auto kMax = std::numeric_limits<int64_t>::max();
  static constexpr auto kMin = std::numeric_limits<int64_t>::min();

  if (a.upper() == kMax || a.upper() + 1 >= b.lower()) {
    if (a.lower() == kMin && (a.upper() == kMax || b.upper() == kMax)) {
      alwaysTrue = true;
      return nullptr;
    }

    return std::make_unique<common::BigintRange>(
        a.lower(), std::max(a.upper(), b.upper()), /*nullAllowed=*/false);
  }
  return nullptr;
}

// Returns a single range that represents "a OR b" or nullptr if no such range
// exists.
// @pre a.lower() <= b.lower()
template <typename T>
std::unique_ptr<common::FloatingPointRange<T>> tryMergeOverlappingRanges(
    const common::FloatingPointRange<T>& a,
    const common::FloatingPointRange<T>& b,
    bool& alwaysTrue) {
  if (!a.upperUnbounded() && !b.lowerUnbounded() &&
      (a.upper() < b.lower() ||
       (a.upper() == b.lower() && a.upperExclusive() && b.lowerExclusive()))) {
    return nullptr;
  }

  const bool lowerUnbounded = a.lowerUnbounded() || b.lowerUnbounded();
  const bool upperUnbounded = a.upperUnbounded() || b.upperUnbounded();

  const T lower = lowerUnbounded ? std::numeric_limits<T>::lowest()
                                 : std::min(a.lower(), b.lower());

  bool lowerExclusive = lowerUnbounded;
  if (!lowerUnbounded) {
    if (a.lower() < b.lower()) {
      lowerExclusive = a.lowerExclusive();
    } else {
      lowerExclusive = a.lowerExclusive() && b.lowerExclusive();
    }
  }

  const T upper = upperUnbounded ? std::numeric_limits<T>::max()
                                 : std::max(a.upper(), b.upper());

  bool upperExclusive = upperUnbounded;
  if (!upperUnbounded) {
    if (a.upper() > b.upper()) {
      upperExclusive = a.upperExclusive();
    } else if (a.upper() < b.upper()) {
      upperExclusive = b.upperExclusive();
    } else {
      upperExclusive = a.upperExclusive() && b.upperExclusive();
    }
  }

  if (lowerUnbounded && upperUnbounded) {
    alwaysTrue = true;
    return nullptr;
  }

  return std::make_unique<common::FloatingPointRange<T>>(
      lower,
      lowerUnbounded,
      lowerExclusive,
      upper,
      upperUnbounded,
      upperExclusive,
      /*nullAllowed=*/false);
}

template <typename T, typename TToMultiRange>
std::unique_ptr<common::Filter> mergeOverlappingDisjuncts(
    std::vector<std::unique_ptr<T>>& ranges,
    bool nullAllowed,
    const TToMultiRange& toMultiRange) {
  std::vector<std::unique_ptr<T>> newRanges;
  newRanges.emplace_back(asUniquePtr<T>(ranges.front()->clone(nullAllowed)));

  for (auto i = 1; i < ranges.size(); i++) {
    bool alwaysTrue = false;
    if (auto merged = tryMergeOverlappingRanges(
            *newRanges.back(), *ranges[i], alwaysTrue)) {
      newRanges.back() = std::move(merged);
    } else {
      if (alwaysTrue) {
        if (nullAllowed) {
          return std::make_unique<common::AlwaysTrue>();
        }
        return isNotNull();
      }
      newRanges.emplace_back(std::move(ranges[i]));
    }
  }

  if (newRanges.size() == 1) {
    return newRanges.front()->clone(nullAllowed);
  }

  return toMultiRange(newRanges, nullAllowed);
}

std::unique_ptr<common::Filter> tryMergeBigintRanges(
    std::vector<std::unique_ptr<common::Filter>>& disjuncts) {
  // Check if all filters are single-value equalities: a = 5. Convert these to
  // an IN list.
  if (std::all_of(disjuncts.begin(), disjuncts.end(), [](const auto& filter) {
        return isBigintRange(filter) &&
            filter->template as<common::BigintRange>()->isSingleValue();
      })) {
    std::vector<int64_t> values;
    values.reserve(disjuncts.size());

    for (auto& filter : disjuncts) {
      values.emplace_back(filter->as<common::BigintRange>()->lower());
    }

    std::sort(values.begin(), values.end());
    values.erase(std::unique(values.begin(), values.end()), values.end());
    return common::createBigintValues(values, isNullAllowed(disjuncts));
  }

  if (!std::all_of(disjuncts.begin(), disjuncts.end(), [](const auto& filter) {
        return isBigintRange(filter) || isBigintMultiRange(filter) ||
            isNegatedBigintRange(filter);
      })) {
    return nullptr;
  }

  const bool nullAllowed = isNullAllowed(disjuncts);

  std::vector<std::unique_ptr<common::BigintRange>> ranges;
  for (auto& filter : disjuncts) {
    if (isBigintRange(filter)) {
      ranges.emplace_back(asBigintRange(filter));
    } else if (isBigintMultiRange(filter)) {
      for (const auto& range :
           filter->as<common::BigintMultiRange>()->ranges()) {
        ranges.emplace_back(std::make_unique<common::BigintRange>(*range));
      }
    } else {
      const auto* negatedRange = filter->as<common::NegatedBigintRange>();
      if (negatedRange->lower() > std::numeric_limits<int64_t>::min()) {
        ranges.emplace_back(
            std::make_unique<common::BigintRange>(
                std::numeric_limits<int64_t>::min(),
                negatedRange->lower() - 1,
                false));
      }
      if (negatedRange->upper() < std::numeric_limits<int64_t>::max()) {
        ranges.emplace_back(
            std::make_unique<common::BigintRange>(
                negatedRange->upper() + 1,
                std::numeric_limits<int64_t>::max(),
                false));
      }
    }
  }

  if (ranges.empty()) {
    if (nullAllowed) {
      return isNull();
    }
    return std::make_unique<common::AlwaysFalse>();
  }

  std::sort(ranges.begin(), ranges.end(), [](const auto& a, const auto& b) {
    return a->lower() < b->lower();
  });

  return mergeOverlappingDisjuncts(
      ranges, nullAllowed, [](auto& newRanges, bool nullAllowed) {
        return std::make_unique<common::BigintMultiRange>(
            std::move(newRanges), nullAllowed);
      });
}

template <typename T>
std::unique_ptr<common::Filter> tryMergeFloatingPointRanges(
    std::vector<std::unique_ptr<common::Filter>>& disjuncts) {
  constexpr auto filterKind = std::is_same_v<T, double>
      ? common::FilterKind::kDoubleRange
      : common::FilterKind::kFloatRange;

  if (!std::all_of(disjuncts.begin(), disjuncts.end(), [](const auto& filter) {
        return filter->is(filterKind);
      })) {
    return nullptr;
  }

  const bool nullAllowed = isNullAllowed(disjuncts);

  std::vector<std::unique_ptr<common::FloatingPointRange<T>>> ranges;
  ranges.reserve(disjuncts.size());
  for (auto& filter : disjuncts) {
    ranges.emplace_back(
        asUniquePtr<common::FloatingPointRange<T>>(std::move(filter)));
  }

  std::sort(ranges.begin(), ranges.end(), [](const auto& a, const auto& b) {
    if (a->lowerUnbounded() && b->lowerUnbounded()) {
      return false;
    }

    if (a->lowerUnbounded()) {
      return true;
    }

    if (b->lowerUnbounded()) {
      return false;
    }

    return a->lower() < b->lower();
  });

  return mergeOverlappingDisjuncts(
      ranges, nullAllowed, [](auto& newRanges, bool nullAllowed) {
        std::vector<std::unique_ptr<common::Filter>> filters;
        filters.reserve(newRanges.size());
        for (auto& range : newRanges) {
          filters.emplace_back(std::move(range));
        }
        return std::make_unique<common::MultiRange>(
            std::move(filters), nullAllowed);
      });
}

std::unique_ptr<common::Filter> tryMergeBytesRanges(
    std::vector<std::unique_ptr<common::Filter>>& disjuncts) {
  if (!std::all_of(disjuncts.begin(), disjuncts.end(), [](const auto& filter) {
        return isBytesRange(filter);
      })) {
    return nullptr;
  }

  const bool nullAllowed = isNullAllowed(disjuncts);
  std::sort(
      disjuncts.begin(), disjuncts.end(), [](const auto& lhs, const auto& rhs) {
        const auto* lhsRange = lhs->template as<common::BytesRange>();
        const auto* rhsRange = rhs->template as<common::BytesRange>();
        if (lhsRange->isLowerUnbounded() && rhsRange->isLowerUnbounded()) {
          return false;
        }
        if (lhsRange->isLowerUnbounded()) {
          return true;
        }
        if (rhsRange->isLowerUnbounded()) {
          return false;
        }
        return lhsRange->lower() < rhsRange->lower();
      });
  return std::make_unique<common::MultiRange>(
      std::move(disjuncts), nullAllowed);
}

std::unique_ptr<common::Filter> tryMergeBytesValues(
    std::vector<std::unique_ptr<common::Filter>>& disjuncts) {
  if (!std::all_of(disjuncts.begin(), disjuncts.end(), [](const auto& filter) {
        return isBytesValues(filter) || isSingleValueBytesRange(filter);
      })) {
    return nullptr;
  }

  const bool nullAllowed = isNullAllowed(disjuncts);

  std::vector<std::string> values;
  for (auto& filter : disjuncts) {
    if (isBytesValues(filter)) {
      const auto* bytesValues = filter->as<common::BytesValues>();
      for (const auto& value : bytesValues->values()) {
        values.push_back(value);
      }
    } else {
      const auto* bytesRange = filter->as<common::BytesRange>();
      values.push_back(bytesRange->lower());
    }
  }

  return std::make_unique<common::BytesValues>(values, nullAllowed);
}

// Merges disjuncts that are CaseMapped filters with the same mapping into one
// whose inner filter merges theirs, e.g. lower(a) = 'x' or lower(a) = 'y' into
// lower(a) in ('x', 'y'). Returns nullptr if the disjuncts are not such filters
// or their inner filters do not merge.
std::unique_ptr<common::Filter> tryMergeCaseMapped(
    const std::vector<std::unique_ptr<common::Filter>>& disjuncts) {
  const auto* first =
      dynamic_cast<const common::CaseMapped*>(disjuncts[0].get());
  if (first == nullptr) {
    return nullptr;
  }
  std::vector<std::unique_ptr<common::Filter>> innerFilters;
  for (const auto& disjunct : disjuncts) {
    const auto* caseMapped =
        dynamic_cast<const common::CaseMapped*>(disjunct.get());
    if (caseMapped == nullptr || caseMapped->mapping() != first->mapping()) {
      return nullptr;
    }
    innerFilters.push_back(caseMapped->inner().clone());
  }
  auto inner =
      ExprToSubfieldFilterParser::makeOrFilter(std::move(innerFilters));
  if (inner == nullptr) {
    return nullptr;
  }
  return common::CaseMapped::wrap(first->mapping(), std::move(inner));
}

} // namespace

// static
std::unique_ptr<common::Filter> ExprToSubfieldFilterParser::makeOrFilter(
    std::vector<std::unique_ptr<common::Filter>> disjuncts) {
  VELOX_CHECK_GE(disjuncts.size(), 2);

  if (std::any_of(disjuncts.begin(), disjuncts.end(), [](const auto& filter) {
        return filter->is(common::FilterKind::kAlwaysTrue);
      })) {
    return alwaysTrue();
  }

  disjuncts.erase(
      std::remove_if(
          disjuncts.begin(),
          disjuncts.end(),
          [](const auto& filter) {
            return filter->is(common::FilterKind::kAlwaysFalse);
          }),
      disjuncts.end());
  if (disjuncts.empty()) {
    return std::make_unique<common::AlwaysFalse>();
  }
  if (disjuncts.size() == 1) {
    return std::move(disjuncts.front());
  }

  const auto firstNotNull =
      std::find_if(disjuncts.begin(), disjuncts.end(), [](const auto& filter) {
        return filter->is(common::FilterKind::kIsNotNull);
      });
  if (firstNotNull != disjuncts.end()) {
    if (std::any_of(disjuncts.begin(), disjuncts.end(), [](const auto& filter) {
          return filter->testNull();
        })) {
      return alwaysTrue();
    }
    return isNotNull();
  }

  const auto firstNull = std::remove_if(
      disjuncts.begin(), disjuncts.end(), [](const auto& filter) {
        return filter->is(common::FilterKind::kIsNull);
      });
  if (firstNull != disjuncts.end()) {
    disjuncts.erase(firstNull, disjuncts.end());
    if (disjuncts.empty()) {
      return isNull();
    }
    if (disjuncts.size() == 1) {
      return disjuncts.front()->clone(true);
    }
    disjuncts.front() = disjuncts.front()->clone(true);
  }

  if (auto merged = tryMergeBigintRanges(disjuncts)) {
    return merged;
  }

  if (auto merged = tryMergeFloatingPointRanges<double>(disjuncts)) {
    return merged;
  }

  if (auto merged = tryMergeFloatingPointRanges<float>(disjuncts)) {
    return merged;
  }

  if (auto merged = tryMergeBytesValues(disjuncts)) {
    return merged;
  }

  if (auto merged = tryMergeBytesRanges(disjuncts)) {
    return merged;
  }

  if (auto merged = tryMergeCaseMapped(disjuncts)) {
    return merged;
  }

  return nullptr;
}

namespace {
std::optional<std::pair<common::Subfield, std::unique_ptr<common::Filter>>>
combine(common::Subfield& subfield, std::unique_ptr<common::Filter>& filter) {
  if (filter != nullptr) {
    return std::make_pair(std::move(subfield), std::move(filter));
  }

  return std::nullopt;
}

// Returns the argument of 'expr' if 'expr' is a call to 'functionName' with a
// single argument of 'argumentKind', nullptr otherwise.
const core::TypedExprPtr* unaryCallArgument(
    const core::TypedExprPtr& expr,
    std::string_view functionName,
    TypeKind argumentKind) {
  if (!expr->isCallKind()) {
    return nullptr;
  }
  const auto* call = expr->asUnchecked<core::CallTypedExpr>();
  if (call->name() != functionName || call->inputs().size() != 1 ||
      call->inputs()[0]->type()->kind() != argumentKind) {
    return nullptr;
  }
  return &call->inputs()[0];
}

// Returns 'call' with its first argument replaced by 'argument'.
core::CallTypedExprPtr replaceFirstArgument(
    const core::CallTypedExpr& call,
    core::TypedExprPtr argument) {
  auto inputs = call.inputs();
  inputs[0] = std::move(argument);
  return std::make_shared<core::CallTypedExpr>(
      call.type(), std::move(inputs), call.name());
}

// Argument and mapping of lower(x) or upper(x).
struct CaseMappingCall {
  core::TypedExprPtr argument;
  // Presto's lower() and upper() use simple Unicode case mapping, which
  // CaseMapped applies.
  common::CaseMapped::Mapping mapping;
};

// Returns x and the mapping if 'expr' is lower(x) or upper(x), or
// std::nullopt.
std::optional<CaseMappingCall> caseMappingCall(const core::TypedExprPtr& expr) {
  using Mapping = common::CaseMapped::Mapping;
  for (const auto& [name, mapping] :
       {std::pair{"lower", Mapping::kLower},
        std::pair{"upper", Mapping::kUpper}}) {
    if (const auto* argument =
            unaryCallArgument(expr, name, TypeKind::VARCHAR)) {
      return CaseMappingCall{*argument, mapping};
    }
  }
  return std::nullopt;
}

// Leaf call with its first argument, lower(x) or upper(x), replaced by x.
struct UnwrappedCaseMapping {
  core::CallTypedExprPtr call;
  // Mapping of lower() or upper().
  common::CaseMapped::Mapping mapping;
};

// Returns 'call' with its first argument unwrapped if that argument is
// lower(x) or upper(x), or std::nullopt otherwise.
std::optional<UnwrappedCaseMapping> unwrapCaseMapping(
    const core::CallTypedExpr& call) {
  auto caseMapping = caseMappingCall(call.inputs()[0]);
  if (!caseMapping.has_value()) {
    return std::nullopt;
  }
  return UnwrappedCaseMapping{
      replaceFirstArgument(call, std::move(caseMapping->argument)),
      caseMapping->mapping};
}

// Returns 'field' with type 'type', or nullptr if 'field' is not a field
// reference.
core::TypedExprPtr retypeField(
    const core::TypedExprPtr& field,
    const TypePtr& type) {
  if (const auto* access =
          dynamic_cast<const core::FieldAccessTypedExpr*>(field.get())) {
    return access->inputs().empty()
        ? std::make_shared<core::FieldAccessTypedExpr>(type, access->name())
        : std::make_shared<core::FieldAccessTypedExpr>(
              type, access->inputs()[0], access->name());
  }
  if (const auto* dereference =
          dynamic_cast<const core::DereferenceTypedExpr*>(field.get())) {
    return std::make_shared<core::DereferenceTypedExpr>(
        type, dereference->inputs()[0], dereference->index());
  }
  return nullptr;
}

// Leaf call with its first argument, length(x), replaced by x retyped as
// BIGINT, so that the call parses into a filter on lengths.
struct UnwrappedLength {
  core::CallTypedExprPtr call;
  // Unit that length() counts.
  common::StringLength::Unit unit;
};

// Returns 'call' with its first argument unwrapped if that argument is
// length(x), or std::nullopt otherwise.
std::optional<UnwrappedLength> unwrapLength(const core::CallTypedExpr& call) {
  using Unit = common::StringLength::Unit;
  // length() counts code points of VARCHAR and bytes of VARBINARY.
  for (const auto& [kind, unit] :
       {std::pair{TypeKind::VARCHAR, Unit::kCodePoints},
        std::pair{TypeKind::VARBINARY, Unit::kBytes}}) {
    if (const auto* argument =
            unaryCallArgument(call.inputs()[0], "length", kind)) {
      if (auto field = retypeField(*argument, BIGINT())) {
        return UnwrappedLength{
            replaceFirstArgument(call, std::move(field)), unit};
      }
      return std::nullopt;
    }
  }
  return std::nullopt;
}

// Returns the value of 'expr' if it is a non-null VARCHAR constant, or
// std::nullopt.
std::optional<std::string> varcharConstant(
    const core::TypedExprPtr& expr,
    core::ExpressionEvaluator* evaluator) {
  if (expr->type()->kind() != TypeKind::VARCHAR) {
    return std::nullopt;
  }
  auto value = toConstant(expr, evaluator);
  if (value == nullptr || value->isNullAt(0)) {
    return std::nullopt;
  }
  return std::string(singleValue<StringView>(value));
}

// One element of a LIKE pattern.
struct LikeToken {
  enum class Kind {
    // A byte that matches itself.
    kLiteral,
    // '_', which matches one character.
    kSingleChar,
    // '%', which matches any number of characters.
    kAnyChars,
  };

  Kind kind;
  // Byte to match if 'kind' is kLiteral.
  char literal{0};
};

// Splits LIKE 'pattern' into tokens the way LIKE reads it: an escape makes the
// next character a literal. Returns std::nullopt for escapes that LIKE rejects,
// i.e. one at the end of 'pattern' or one before a character other than '%',
// '_' and the escape.
std::optional<std::vector<LikeToken>> likeTokens(
    std::string_view pattern,
    std::optional<char> escape) {
  std::vector<LikeToken> tokens;
  for (size_t i = 0; i < pattern.size(); ++i) {
    const char c = pattern[i];
    if (c == escape) {
      if (i + 1 == pattern.size()) {
        return std::nullopt;
      }
      const char escaped = pattern[++i];
      if (escaped != '%' && escaped != '_' && escaped != c) {
        return std::nullopt;
      }
      tokens.push_back({LikeToken::Kind::kLiteral, escaped});
    } else if (c == '%') {
      tokens.push_back({LikeToken::Kind::kAnyChars});
    } else if (c == '_') {
      tokens.push_back({LikeToken::Kind::kSingleChar});
    } else {
      tokens.push_back({LikeToken::Kind::kLiteral, c});
    }
  }
  return tokens;
}

// Returns the tokens of the pattern of 'call' if it is x LIKE pattern
// [ESCAPE escape] on a VARCHAR with a constant pattern and escape, or
// std::nullopt.
std::optional<std::vector<LikeToken>> likeCallTokens(
    const core::CallTypedExpr& call,
    core::ExpressionEvaluator* evaluator) {
  const auto& inputs = call.inputs();
  if (call.name() != "like" || inputs.size() < 2 ||
      inputs[0]->type()->kind() != TypeKind::VARCHAR) {
    return std::nullopt;
  }
  const auto pattern = varcharConstant(inputs[1], evaluator);
  if (!pattern.has_value()) {
    return std::nullopt;
  }
  std::optional<char> escape;
  if (inputs.size() > 2) {
    const auto escapeString = varcharConstant(inputs[2], evaluator);
    if (!escapeString.has_value() || escapeString->size() != 1) {
      return std::nullopt;
    }
    escape = escapeString->front();
  }
  return likeTokens(pattern.value(), escape);
}

// Returns a filter on the lengths of strings that match a LIKE pattern of
// 'tokens', or nullptr if the pattern does not restrict the length. Literal
// bytes match themselves in all LIKE implementations. Each '_' matches one byte
// of an ASCII string and at least one byte of other strings: on invalid UTF-8,
// some implementations match a lead byte with any number of continuation
// bytes. So a pattern with '_' bounds only ASCII strings from above.
std::unique_ptr<common::Filter> likeLengthFilter(
    const std::vector<LikeToken>& tokens) {
  using Unit = common::StringLength::Unit;
  int64_t minLength{0};
  bool hasSingleChar{false};
  bool hasAnyChars{false};
  for (const auto& token : tokens) {
    switch (token.kind) {
      case LikeToken::Kind::kLiteral:
        ++minLength;
        break;
      case LikeToken::Kind::kSingleChar:
        ++minLength;
        hasSingleChar = true;
        break;
      case LikeToken::Kind::kAnyChars:
        hasAnyChars = true;
        break;
    }
  }
  if (hasAnyChars) {
    if (minLength == 0) {
      return nullptr;
    }
    // Without an upper bound, both units pass the same strings. kBytes lets
    // the filter merge with other implied filters.
    return std::make_unique<common::StringLength>(
        Unit::kBytes, greaterThanOrEqual(minLength));
  }
  return std::make_unique<common::StringLength>(
      hasSingleChar ? Unit::kAsciiBytes : Unit::kBytes, equal(minLength));
}

// Returns the literal of a LIKE pattern '%literal%' of 'tokens' without other
// wildcards, which LIKE matches as a substring of bytes, or std::nullopt.
std::optional<std::string> likeSubstring(const std::vector<LikeToken>& tokens) {
  if (tokens.size() < 3 || tokens.front().kind != LikeToken::Kind::kAnyChars ||
      tokens.back().kind != LikeToken::Kind::kAnyChars) {
    return std::nullopt;
  }
  std::string substring;
  for (size_t i = 1; i + 1 < tokens.size(); ++i) {
    if (tokens[i].kind != LikeToken::Kind::kLiteral) {
      return std::nullopt;
    }
    substring.push_back(tokens[i].literal);
  }
  return substring;
}

// String and substring of a call that holds exactly when the string contains
// the substring.
struct ContainedSubstring {
  core::TypedExprPtr string;
  std::string substring;
};

// Returns x and s if 'call' holds exactly when x contains s: x LIKE '%s%', or
// strpos(x, s) > 0, >= 1 or <> 0, since strpos() returns 0 if and only if x
// does not contain s. 'negated' applies to 'call'.
std::optional<ContainedSubstring> containedSubstring(
    const core::CallTypedExpr& call,
    core::ExpressionEvaluator* evaluator,
    bool negated) {
  const auto& inputs = call.inputs();
  if (call.name() == "like") {
    if (negated) {
      return std::nullopt;
    }
    const auto tokens = likeCallTokens(call, evaluator);
    if (!tokens.has_value()) {
      return std::nullopt;
    }
    auto substring = likeSubstring(tokens.value());
    if (!substring.has_value()) {
      return std::nullopt;
    }
    return ContainedSubstring{inputs[0], std::move(substring.value())};
  }

  auto kind = comparisonKind(call.name());
  if (!kind.has_value() || inputs.size() != 2 || !inputs[0]->isCallKind() ||
      inputs[1]->type()->kind() != TypeKind::BIGINT) {
    return std::nullopt;
  }
  const auto* strpos = inputs[0]->asUnchecked<core::CallTypedExpr>();
  if (strpos->name() != "strpos" || strpos->inputs().size() != 2) {
    return std::nullopt;
  }
  const auto bound = toConstant(inputs[1], evaluator);
  if (bound == nullptr || bound->isNullAt(0)) {
    return std::nullopt;
  }
  if (negated) {
    kind = negateComparison(kind.value());
  }
  const auto value = singleValue<int64_t>(bound);
  const bool found = (kind == ComparisonKind::kGt && value == 0) ||
      (kind == ComparisonKind::kGte && value == 1) ||
      (kind == ComparisonKind::kNeq && value == 0);
  if (!found) {
    return std::nullopt;
  }
  auto substring = varcharConstant(strpos->inputs()[1], evaluator);
  if (!substring.has_value()) {
    return std::nullopt;
  }
  return ContainedSubstring{strpos->inputs()[0], std::move(substring.value())};
}
} // namespace

std::optional<std::pair<common::Subfield, std::unique_ptr<common::Filter>>>
PrestoExprToSubfieldFilterParser::leafCallToSubfieldFilter(
    const core::CallTypedExpr& call,
    core::ExpressionEvaluator* evaluator,
    bool negated) {
  if (call.inputs().empty()) {
    return std::nullopt;
  }

  // Translates a leaf call on length(x), lower(x) or upper(x), e.g.
  // length(x) between 1 and 3 or lower(x) in ('a', 'b'), into a StringLength
  // or CaseMapped filter on x around the filter for the call on x.
  if (auto unwrapped = unwrapLength(call)) {
    auto result =
        leafCallToSubfieldFilter(*unwrapped->call, evaluator, negated);
    if (result.has_value()) {
      result->second = common::StringLength::wrap(
          unwrapped->unit, std::move(result->second));
    }
    return result;
  }
  if (auto unwrapped = unwrapCaseMapping(call)) {
    // Recursing on lower(upper(x)) would give CaseMapped(kLower,
    // CaseMapped(kUpper, ...)), which applies lower() before upper(), the
    // reverse of the expression. Nested mappings stay in the remaining filter
    // instead.
    if (unwrapCaseMapping(*unwrapped->call).has_value()) {
      return std::nullopt;
    }
    auto result =
        leafCallToSubfieldFilter(*unwrapped->call, evaluator, negated);
    if (result.has_value()) {
      result->second = common::CaseMapped::wrap(
          unwrapped->mapping, std::move(result->second));
    }
    return result;
  }

  // x LIKE '%s%' and strpos(x, s) > 0 search the bytes of x for s, as
  // StringContains does. lower(x) LIKE '%s%' is unwrapped above, but
  // strpos(lower(x), s) has the mapping inside its first argument, so it is
  // unwrapped here.
  if (auto contained = containedSubstring(call, evaluator, negated)) {
    auto caseMapping = caseMappingCall(contained->string);
    const auto& string =
        caseMapping.has_value() ? caseMapping->argument : contained->string;
    common::Subfield subfield;
    if (!toSubfield(string.get(), subfield)) {
      return std::nullopt;
    }
    std::unique_ptr<common::Filter> filter;
    if (contained->substring.empty()) {
      // strpos(x, '') returns 1 for every non-null x.
      filter = isNotNull();
    } else {
      filter = std::make_unique<common::StringContains>(
          std::move(contained->substring), /*nullAllowed=*/false);
    }
    if (caseMapping.has_value()) {
      filter =
          common::CaseMapped::wrap(caseMapping->mapping, std::move(filter));
    }
    return std::make_pair(std::move(subfield), std::move(filter));
  }

  const auto* leftSide = call.inputs()[0].get();
  VELOX_CHECK_NOT_NULL(leftSide);

  common::Subfield subfield;
  if (auto kind = comparisonKind(call.name())) {
    if (negated) {
      *kind = negateComparison(*kind);
    }

    auto makeComparisonFilter = [&](ComparisonKind comparison,
                                    const core::TypedExprPtr& valueExpr) {
      switch (comparison) {
        case ComparisonKind::kEq:
          return makeEqualFilter(valueExpr, evaluator);
        case ComparisonKind::kNeq:
          return makeNotEqualFilter(valueExpr, evaluator);
        case ComparisonKind::kLt:
          return makeLessThanFilter(valueExpr, evaluator);
        case ComparisonKind::kLte:
          return makeLessThanOrEqualFilter(valueExpr, evaluator);
        case ComparisonKind::kGt:
          return makeGreaterThanFilter(valueExpr, evaluator);
        case ComparisonKind::kGte:
          return makeGreaterThanOrEqualFilter(valueExpr, evaluator);
      }
      VELOX_UNREACHABLE();
    };

    auto extractThroughIntegralWideningCast =
        [&](const core::ITypedExpr* fieldExpr,
            const core::TypedExprPtr& valueExpr,
            ComparisonKind comparison)
        -> std::optional<
            std::pair<common::Subfield, std::unique_ptr<common::Filter>>> {
      auto cast = unwrapIntegralWideningCast(fieldExpr);
      if (!cast.has_value()) {
        return std::nullopt;
      }

      if (cast->sourceExpr == nullptr) {
        return std::nullopt;
      }
      common::Subfield castSubfield;
      if (!toSubfield(cast->sourceExpr, castSubfield)) {
        return std::nullopt;
      }
      auto filter = makeIntegralWideningCastFilter(
          *cast, valueExpr, comparison, evaluator);
      if (filter == nullptr) {
        return std::nullopt;
      }
      return std::make_pair(std::move(castSubfield), std::move(filter));
    };

    if (auto result = extractThroughIntegralWideningCast(
            leftSide, call.inputs()[1], *kind)) {
      return result;
    }
    const auto* rightSide = call.inputs()[1].get();
    VELOX_CHECK_NOT_NULL(rightSide);
    if (auto result = extractThroughIntegralWideningCast(
            rightSide, call.inputs()[0], flipComparison(*kind))) {
      return result;
    }
    if (toSubfield(leftSide, subfield)) {
      auto filter = makeComparisonFilter(*kind, call.inputs()[1]);
      return combine(subfield, filter);
    }
  } else if (call.name() == "between") {
    if (toSubfield(leftSide, subfield)) {
      auto filter = makeBetweenFilter(
          call.inputs()[1], call.inputs()[2], evaluator, negated);
      return combine(subfield, filter);
    }
  } else if (call.name() == "in") {
    const auto elementKind = leftSide->type()->kind();
    if (isSupportedInElementKind(elementKind) &&
        toSubfield(leftSide, subfield)) {
      const auto& listType = call.inputs()[1]->type();

      // Two encodings. The element kind is a supported scalar, so a second
      // argument of that same kind is the varargs form `in(col, a, b, c, ...)`;
      // otherwise it is the `in(col, <ARRAY(colType) constant>)` form.
      std::unique_ptr<common::Filter> filter;
      if (listType->kind() == elementKind) {
        filter = makeVarargsInFilter(
            std::span<const core::TypedExprPtr>(call.inputs()).subspan(1),
            evaluator,
            negated);
      } else if (listType->isUnknown()) {
        filter = std::make_unique<common::AlwaysFalse>();
      } else {
        VELOX_CHECK(listType->isArray());
        if (listType->childAt(0)->isUnknown()) {
          filter = std::make_unique<common::AlwaysFalse>();
        } else {
          filter = makeInFilter(call.inputs()[1], evaluator, negated);
        }
      }
      if (filter) {
        return combine(subfield, filter);
      }
    }
  } else if (call.name() == "is_null") {
    if (toSubfield(leftSide, subfield)) {
      if (negated) {
        return std::make_pair(std::move(subfield), isNotNull());
      }
      return std::make_pair(std::move(subfield), isNull());
    }
  }
  return std::nullopt;
}

std::optional<std::pair<common::Subfield, std::unique_ptr<common::Filter>>>
PrestoExprToSubfieldFilterParser::leafCallToImpliedSubfieldFilter(
    const core::CallTypedExpr& call,
    core::ExpressionEvaluator* evaluator,
    bool negated) {
  if (negated) {
    return std::nullopt;
  }
  const auto tokens = likeCallTokens(call, evaluator);
  if (!tokens.has_value()) {
    return std::nullopt;
  }
  common::Subfield subfield;
  if (!toSubfield(call.inputs()[0].get(), subfield)) {
    return std::nullopt;
  }
  auto lengthFilter = likeLengthFilter(tokens.value());
  if (lengthFilter == nullptr) {
    return std::nullopt;
  }
  return std::make_pair(std::move(subfield), std::move(lengthFilter));
}

} // namespace facebook::velox::exec
