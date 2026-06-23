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

#include "velox/expression/CastHooks.h"
#include "velox/expression/EvalCtx.h"
#include "velox/type/Timestamp.h"
#include "velox/type/tz/TimeZoneMap.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/vector/SimpleVector.h"

namespace facebook::velox::exec {

/// Cast hooks interface used by CastExprV2 to control dialect-specific
/// cast behavior (Presto, Spark, legacy).  Extends V1's CastHooks with
/// whole-column entry points so concrete implementations can vectorize
/// per-row conversion.  V1's CastHooks dispatches every row through a
/// virtual call, which blocks inlining of the scalar parser and
/// prevents the compiler from fusing adjacent rows; moving the loop
/// inside the hook lets the concrete call type devirtualize the parser
/// and gives the compiler a chance to auto-vectorize the surrounding
/// code.
///
/// CastHooksV2 inherits from CastHooks so a shared_ptr<CastHooksV2>
/// can be passed wherever V1's CastOperator API expects a
/// shared_ptr<CastHooks>, without changing the V1 interface.  The
/// scalar virtuals come from CastHooks unchanged; CastHooksV2 adds
/// whole-column virtuals (suffix Vector) on top.  CastExprV2's per-row
/// apply path calls the inherited scalar hooks during the migration;
/// it will move to the vector hooks once the apply path is rewritten
/// for whole-column dispatch.
class CastHooksV2 : public CastHooks {
 public:
  // ===== Vector surface. =====
  //
  // Whole-column entry points.  Concrete implementations own the row
  // loop, which devirtualizes the scalar parser at the call site.
  // Errors are reported via EvalCtx::setStatus(row, error); successful
  // rows are written into 'result' via FlatVector::set.  The cast
  // expression owns the null-vs-propagate policy and converts recorded
  // errors to nulls at end-of-apply based on isTryCast.

  /// VARCHAR -> TIMESTAMP, whole column.
  virtual void castStringToTimestampVector(
      const SelectivityVector& rows,
      const SimpleVector<StringView>& input,
      FlatVector<Timestamp>& result,
      EvalCtx& context) const = 0;

  /// VARCHAR -> DATE, whole column.
  virtual void castStringToDateVector(
      const SelectivityVector& rows,
      const SimpleVector<StringView>& input,
      FlatVector<int32_t>& result,
      EvalCtx& context) const = 0;

  /// VARCHAR -> REAL, whole column.
  virtual void castStringToRealVector(
      const SelectivityVector& rows,
      const SimpleVector<StringView>& input,
      FlatVector<float>& result,
      EvalCtx& context) const = 0;

  /// VARCHAR -> DOUBLE, whole column.
  virtual void castStringToDoubleVector(
      const SelectivityVector& rows,
      const SimpleVector<StringView>& input,
      FlatVector<double>& result,
      EvalCtx& context) const = 0;

  /// DATE -> TIMESTAMP, whole column.  Builds the Timestamp at
  /// midnight UTC; when 'timeZone' is non-null the result is shifted
  /// from local-time-at-that-zone to GMT.
  virtual void castDateToTimestampVector(
      const SelectivityVector& rows,
      const SimpleVector<int32_t>& input,
      FlatVector<Timestamp>& result,
      const tz::TimeZone* timeZone) const = 0;

  /// Bulk local-to-GMT shift on a Timestamp column.  Mirrors the
  /// per-row scalar castDateTimestampToGMT.
  virtual void castDateTimestampToGMTVector(
      const SelectivityVector& rows,
      FlatVector<Timestamp>& timestamps,
      const tz::TimeZone& timeZone) const = 0;

  /// INT (any width) -> TIMESTAMP, whole column.  Concrete dialects
  /// that disallow this cast record a user error for every selected
  /// row in one call.
  virtual void castIntToTimestampVector(
      const SelectivityVector& rows,
      EvalCtx& context) const = 0;

  /// BOOLEAN -> TIMESTAMP, whole column.
  virtual void castBooleanToTimestampVector(
      const SelectivityVector& rows,
      EvalCtx& context) const = 0;

  /// TIMESTAMP -> INT, whole column.
  virtual void castTimestampToIntVector(
      const SelectivityVector& rows,
      EvalCtx& context) const = 0;

  /// REAL/DOUBLE -> TIMESTAMP, whole column.
  virtual void castDoubleToTimestampVector(
      const SelectivityVector& rows,
      EvalCtx& context) const = 0;
};

} // namespace facebook::velox::exec
