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

#include <algorithm>
#include <cmath>
#include <limits>

#include <double-conversion/double-conversion.h>
#include <folly/Expected.h>

#include "velox/expression/CastHooksV2.h"
#include "velox/functions/lib/string/StringImpl.h"
#include "velox/type/TimestampConversion.h"

namespace facebook::velox::exec {

/// Presto implementation of CastHooksV2.  All conversion logic lives
/// in this header so vector hooks call concrete, inlined helpers
/// inside their row loops rather than going through a virtual call
/// (which is what V1's PrestoCastHooks dispatch costs per row).  This
/// is what makes the V2 cast path "really vectorized": the compiler
/// sees the parser body at the call site, can inline it, and is free
/// to fuse adjacent rows.
///
/// State is identical to PrestoCastHooks (legacyCast flag + cached
/// TimestampToStringOptions) but PrestoCastHooks itself is no longer
/// composed or called.  The scalar overrides required by the inherited
/// CastHooks interface (for V1 CastOperator compatibility) call the
/// same private helpers as the vector overrides, so behavior matches
/// V1 row-for-row by construction.
class PrestoCastHooksV2 : public CastHooksV2 {
 public:
  explicit PrestoCastHooksV2(const core::QueryConfig& config)
      : legacyCast_(config.isLegacyCast()) {
    if (!legacyCast_) {
      options_.zeroPaddingYear = true;
      options_.dateTimeSeparator = ' ';
      const auto sessionTzName = config.sessionTimezone();
      if (config.adjustTimestampToTimezone() && !sessionTzName.empty()) {
        options_.timeZone = tz::locateZone(sessionTzName);
      }
    }
  }

  // ----- Scalar overrides (CastHooks). -----
  //
  // The cast expression's per-row apply path (V1 CastOperator, and
  // V2 paths that haven't been vectorized yet) reaches these via the
  // base CastHooks interface, so they remain available.  Every
  // override forwards to the same private helper the vector path uses.

  Expected<Timestamp> castStringToTimestamp(
      const StringView& view) const override {
    return doStringToTimestamp(view);
  }

  Expected<Timestamp> castIntToTimestamp(int64_t /*seconds*/) const override {
    return folly::makeUnexpected(kIntToTimestampError);
  }

  Expected<int64_t> castTimestampToInt(
      Timestamp /*timestamp*/) const override {
    return folly::makeUnexpected(kTimestampToIntError);
  }

  Expected<std::optional<Timestamp>> castDoubleToTimestamp(
      double /*seconds*/) const override {
    return folly::makeUnexpected(kDoubleToTimestampError);
  }

  Expected<int32_t> castStringToDate(
      const StringView& dateString) const override {
    return doStringToDate(dateString);
  }

  Expected<float> castStringToReal(const StringView& data) const override {
    return doStringToFloating<float>(data);
  }

  Expected<double> castStringToDouble(const StringView& data) const override {
    return doStringToFloating<double>(data);
  }

  Expected<Timestamp> castBooleanToTimestamp(bool /*seconds*/) const override {
    return folly::makeUnexpected(kBoolToTimestampError);
  }

  StringView removeWhiteSpaces(const StringView& view) const override {
    return view;
  }

  void castDateTimestampToGMT(
      Timestamp& timestamp,
      const tz::TimeZone& timeZone) const override {
    timestamp.toGMT(timeZone);
  }

  // ----- Configuration accessors. -----

  const TimestampToStringOptions& timestampToStringOptions() const override {
    return options_;
  }

  bool truncate() const override {
    return false;
  }

  bool applyTryCastRecursively() const override {
    return false;
  }

  bool isScientific() const override {
    return false;
  }

  PolicyType getPolicy() const override {
    return legacyCast_ ? PolicyType::LegacyCastPolicy
                       : PolicyType::PrestoCastPolicy;
  }

  // ----- Vector overrides. -----
  //
  // Each loop body is the inlined scalar work; the parser is visible
  // to the compiler at this call site.  Errors are recorded via
  // EvalCtx::setStatus / setStatuses and the cast expression converts
  // them to nulls at end-of-apply when try_cast is in effect.

  void castStringToTimestampVector(
      const SelectivityVector& rows,
      const SimpleVector<StringView>& input,
      FlatVector<Timestamp>& result,
      EvalCtx& context) const override {
    castStringVector(rows, input, result, context, [this](StringView view) {
      return doStringToTimestamp(view);
    });
  }

  void castStringToDateVector(
      const SelectivityVector& rows,
      const SimpleVector<StringView>& input,
      FlatVector<int32_t>& result,
      EvalCtx& context) const override {
    castStringVector(rows, input, result, context, [this](StringView view) {
      return doStringToDate(view);
    });
  }

  void castStringToRealVector(
      const SelectivityVector& rows,
      const SimpleVector<StringView>& input,
      FlatVector<float>& result,
      EvalCtx& context) const override {
    castStringVector(rows, input, result, context, [](StringView view) {
      return doStringToFloating<float>(view);
    });
  }

  void castStringToDoubleVector(
      const SelectivityVector& rows,
      const SimpleVector<StringView>& input,
      FlatVector<double>& result,
      EvalCtx& context) const override {
    castStringVector(rows, input, result, context, [](StringView view) {
      return doStringToFloating<double>(view);
    });
  }

  void castDateToTimestampVector(
      const SelectivityVector& rows,
      const SimpleVector<int32_t>& input,
      FlatVector<Timestamp>& result,
      const tz::TimeZone* timeZone) const override {
    static constexpr int64_t kMillisPerDay = 86'400'000;
    if (timeZone == nullptr) {
      rows.applyToSelected([&](vector_size_t row) {
        result.set(
            row, Timestamp::fromMillis(input.valueAt(row) * kMillisPerDay));
      });
    } else {
      rows.applyToSelected([&](vector_size_t row) {
        auto ts = Timestamp::fromMillis(input.valueAt(row) * kMillisPerDay);
        ts.toGMT(*timeZone);
        result.set(row, ts);
      });
    }
  }

  void castDateTimestampToGMTVector(
      const SelectivityVector& rows,
      FlatVector<Timestamp>& timestamps,
      const tz::TimeZone& timeZone) const override {
    rows.applyToSelected([&](vector_size_t row) {
      auto ts = timestamps.valueAt(row);
      ts.toGMT(timeZone);
      timestamps.set(row, ts);
    });
  }

  void castIntToTimestampVector(
      const SelectivityVector& rows,
      EvalCtx& context) const override {
    context.setStatuses(rows, kIntToTimestampError);
  }

  void castBooleanToTimestampVector(
      const SelectivityVector& rows,
      EvalCtx& context) const override {
    context.setStatuses(rows, kBoolToTimestampError);
  }

  void castTimestampToIntVector(
      const SelectivityVector& rows,
      EvalCtx& context) const override {
    context.setStatuses(rows, kTimestampToIntError);
  }

  void castDoubleToTimestampVector(
      const SelectivityVector& rows,
      EvalCtx& context) const override {
    context.setStatuses(rows, kDoubleToTimestampError);
  }

 private:
  // VARCHAR -> TIMESTAMP scalar parser.  Same algorithm as
  // PrestoCastHooks::castStringToTimestamp; inlined here so the
  // compiler can see the body inside castStringToTimestampVector.
  Expected<Timestamp> doStringToTimestamp(const StringView& view) const {
    const auto conversionResult = util::fromTimestampWithTimezoneString(
        view.data(),
        view.size(),
        legacyCast_ ? util::TimestampParseMode::kLegacyCast
                    : util::TimestampParseMode::kPrestoCast);
    if (conversionResult.hasError()) {
      return folly::makeUnexpected(conversionResult.error());
    }
    return util::fromParsedTimestampWithTimeZone(
        conversionResult.value(), options_.timeZone);
  }

  // VARCHAR -> DATE scalar parser.  Strict ISO 8601:
  // [+-]YYYY-MM-DD only, matching V1.
  static Expected<int32_t> doStringToDate(const StringView& dateString) {
    return util::fromDateString(dateString, util::ParseMode::kPrestoCast);
  }

  // VARCHAR -> {REAL, DOUBLE} scalar parser.  Reproduces V1's
  // doCastToFloatingPoint exactly: trim leading whitespace, fail on
  // all-whitespace, parse via double-conversion with NaN as the junk
  // value, then re-error if the parser consumed zero characters.
  template <typename T>
  static Expected<T> doStringToFloating(const StringView& data) {
    static const T kNan = std::numeric_limits<T>::quiet_NaN();
    static double_conversion::StringToDoubleConverter
        stringToDoubleConverter{
            double_conversion::StringToDoubleConverter::ALLOW_TRAILING_SPACES,
            /*empty_string_value*/ kNan,
            /*junk_string_value*/ kNan,
            "Infinity",
            "NaN"};
    auto* begin = std::find_if_not(data.begin(), data.end(), [](char c) {
      return functions::stringImpl::isAsciiWhiteSpace(c);
    });
    auto length = data.end() - begin;
    if (length == 0) {
      return folly::makeUnexpected(Status::UserError());
    }
    int processedCharactersCount;
    T result;
    if constexpr (std::is_same_v<T, float>) {
      result = stringToDoubleConverter.StringToFloat(
          begin, length, &processedCharactersCount);
    } else {
      result = stringToDoubleConverter.StringToDouble(
          begin, length, &processedCharactersCount);
    }
    if UNLIKELY (processedCharactersCount == 0) {
      return folly::makeUnexpected(Status::UserError());
    }
    return result;
  }

  // Per-row loop body shared by every VARCHAR -> primitive vector
  // hook.  Mirrors V1 castKernel's preprocessing: trim (no-op in
  // Presto), short-circuit empty strings, dispatch to the inlined
  // scalar helper.
  template <typename T, typename ScalarFn>
  void castStringVector(
      const SelectivityVector& rows,
      const SimpleVector<StringView>& input,
      FlatVector<T>& result,
      EvalCtx& context,
      ScalarFn&& scalarFn) const {
    rows.applyToSelected([&](vector_size_t row) {
      const auto view = removeWhiteSpaces(input.valueAt(row));
      if (view.size() == 0) {
        context.setStatus(row, Status::UserError("Empty string"));
        return;
      }
      auto castResult = scalarFn(view);
      if (castResult.hasError()) {
        context.setStatus(row, castResult.error());
      } else {
        result.set(row, castResult.value());
      }
    });
  }

  // Cached Status objects for the four casts Presto disallows.  Held
  // as static locals so the bulk EvalCtx::setStatuses calls reuse one
  // Status per error category instead of allocating per call.
  static inline const Status kIntToTimestampError{
      Status::UserError("Conversion to Timestamp is not supported")};
  static inline const Status kBoolToTimestampError{
      Status::UserError("Conversion to Timestamp is not supported")};
  static inline const Status kDoubleToTimestampError{
      Status::UserError("Conversion to Timestamp is not supported")};
  static inline const Status kTimestampToIntError{
      Status::UserError("Conversion from Timestamp to Int is not supported")};

  const bool legacyCast_;
  TimestampToStringOptions options_ = {
      .precision = TimestampToStringOptions::Precision::kMilliseconds};
};

} // namespace facebook::velox::exec
