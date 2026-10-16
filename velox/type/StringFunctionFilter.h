/*
 * Copyright (c) International Business Machines Corporation
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

#include "velox/type/Filter.h"

namespace facebook::velox::common {

/// Passes strings whose length passes an integer filter. The length is
/// measured in one of several units, so the filter can be derived from
/// different string predicates:
///   length(x) = 3 -> {kCodePoints, BigintRange(3, 3)}
///   x LIKE 'a_%' -> {kBytes, BigintRange(2, max)}
///   x LIKE 'a_c' -> {kAsciiBytes, BigintRange(3, 3)}
/// CaseMapped uses kCaseMappedCodePoints to check lower(x) IN ('a', 'bcd')
/// without mapping values whose length differs.
class StringLength final : public Filter {
 public:
  enum class Unit {
    /// Number of bytes.
    kBytes,
    /// Number of bytes that are not UTF-8 continuation bytes, i.e. the number
    /// of code points of valid UTF-8.
    kCodePoints,
    /// Number of code points after simple Unicode case mapping, which maps
    /// each code point to one code point with no locale or context rules, so
    /// lowercasing and uppercasing give the same count. Equals kCodePoints for
    /// valid UTF-8 but can differ for invalid UTF-8.
    kCaseMappedCodePoints,
    /// Number of bytes of an ASCII string. A string with non-ASCII bytes
    /// passes if any length up to its number of bytes passes, because LIKE
    /// implementations disagree on how many bytes '_' matches in invalid
    /// UTF-8.
    kAsciiBytes,
  };

  VELOX_DECLARE_EMBEDDED_ENUM_NAME(Unit);

  /// Returns a filter on strings whose length in 'unit' passes 'inner'. A
  /// length is null only for a null string, so a filter that only tests for
  /// null, e.g. AlwaysFalse or IsNotNull, is returned as is.
  static std::unique_ptr<Filter> wrap(Unit unit, std::unique_ptr<Filter> inner);

  /// @param unit Unit of the lengths that 'inner' tests.
  /// @param inner Filter on lengths. Nulls pass if 'inner' allows nulls.
  StringLength(Unit unit, std::unique_ptr<Filter> inner);

  /// Copies 'other' with 'nullAllowed'. Used by clone().
  StringLength(const StringLength& other, bool nullAllowed)
      : Filter(true, nullAllowed, FilterKind::kStringLength),
        unit_(other.unit_),
        inner_(other.inner_->clone(nullAllowed)) {}

  folly::dynamic serialize() const override;

  static std::unique_ptr<Filter> create(const folly::dynamic& obj);

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    return std::make_unique<StringLength>(
        *this, nullAllowed.value_or(nullAllowed_));
  }

  bool testNonNull() const final {
    return true;
  }

  bool hasTestLength() const final {
    return true;
  }

  /// Tests the length in bytes. Exact for kBytes. A string has at most as many
  /// code points as bytes, so for the other units this tests whether any
  /// length up to 'length' passes.
  bool testLength(int32_t length) const final;

  bool testBytes(const char* value, int32_t length) const final;

  /// The byte order of strings does not bound their lengths, so min and max
  /// only rule out a range that has a single value.
  bool testBytesRange(
      std::optional<std::string_view> min,
      std::optional<std::string_view> max,
      bool hasNull) const final;

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

  /// Returns true if mergeWith() merges 'other' into an equivalent filter:
  /// 'other' only tests for null, is an IN-list, or is a StringLength in the
  /// same unit. For kAsciiBytes, both inner filters must be overlapping
  /// BigintRanges.
  bool canMergeWith(const Filter& other) const;

  Unit unit() const {
    return unit_;
  }

  const Filter& inner() const {
    return *inner_;
  }

  std::string toString() const final;

  bool testingEquals(const Filter& other) const final;

 private:
  const Unit unit_;
  // Filter on the length in 'unit_', e.g. BigintRange(3, 3) for
  // length(x) = 3.
  const std::unique_ptr<Filter> inner_;
};

/// Passes strings for which 'inner' passes after simple Unicode case mapping:
/// each code point is lowercased or uppercased on its own, with no locale or
/// context rules. Engines whose lower() or upper() apply such rules, e.g.
/// Turkish dotted I or Greek final sigma, must not translate them to this
/// filter. Used to push down predicates like lower(x) = 'abc' or
/// upper(x) NOT IN ('A', 'B'). Readers evaluate filters once per distinct value
/// of a dictionary-encoded column, so the mapping runs on the dictionary
/// instead of on every row.
class CaseMapped final : public Filter {
 public:
  enum class Mapping {
    kLower,
    kUpper,
  };

  VELOX_DECLARE_EMBEDDED_ENUM_NAME(Mapping);

  /// For each code point of a value, the UTF-8 encodings of the code points
  /// that map to it, in byte order. A string maps to the value if and only if
  /// it is made of one entry per position. E.g. under lower(), 'ab-1' has
  /// {{"A", "a"}, {"B", "b"}, {"-"}, {"1"}}, so 'AB-1', 'Ab-1', 'aB-1' and
  /// 'ab-1' map to it. The entry for 'k' also holds the Kelvin sign U+212A,
  /// and 'A' has no entries because nothing lowercases to it.
  using Preimages = std::vector<std::vector<std::string>>;

  /// Returns a filter on strings that pass 'inner' after 'mapping'. Mapping
  /// keeps nulls and non-nulls, so a filter that only tests for null, e.g.
  /// AlwaysFalse or IsNotNull, is returned as is.
  static std::unique_ptr<Filter> wrap(
      Mapping mapping,
      std::unique_ptr<Filter> inner);

  /// @param mapping Function to apply to values before testing them with
  /// 'inner'.
  /// @param inner Filter on mapped values. Nulls pass if 'inner' allows nulls.
  CaseMapped(Mapping mapping, std::unique_ptr<Filter> inner);

  /// Copies 'other' with 'nullAllowed'. Used by clone().
  CaseMapped(const CaseMapped& other, bool nullAllowed);

  folly::dynamic serialize() const override;

  static std::unique_ptr<Filter> create(const folly::dynamic& obj);

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    return std::make_unique<CaseMapped>(
        *this, nullAllowed.value_or(nullAllowed_));
  }

  bool testNonNull() const final {
    return true;
  }

  bool hasTestLength() const final {
    return lengthFilter_ != nullptr;
  }

  bool testLength(int32_t length) const final {
    return lengthFilter_ == nullptr || lengthFilter_->testLength(length);
  }

  bool testBytes(const char* value, int32_t length) const final;

  /// Returns false if no string in ['min', 'max'] maps to a value of an
  /// IN-list 'inner'. 'min' and 'max' bound the strings before mapping, which
  /// do not bound the mapped strings: 'AB-1' and 'ab-1' both map to 'ab-1'
  /// under lower() but sort far apart, because uppercase letters come first.
  /// So this checks whether some preimage of a value is in the range. A string
  /// with a non-ASCII byte after a matching prefix may be malformed UTF-8 and
  /// is assumed to map to any value. Returns true for other 'inner' filters.
  bool testBytesRange(
      std::optional<std::string_view> min,
      std::optional<std::string_view> max,
      bool hasNull) const final;

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

  Mapping mapping() const {
    return mapping_;
  }

  const Filter& inner() const {
    return *inner_;
  }

  std::string toString() const final;

  bool testingEquals(const Filter& other) const final;

 private:
  const Mapping mapping_;
  // Filter on the case-mapped value, e.g. BytesValues {'aws rds'} for
  // lower(x) = 'aws rds'.
  const std::unique_ptr<Filter> inner_;
  // Lengths of the values accepted by an IN-list 'inner', in case-mapped code
  // points. Mapping keeps the number of code points, so checking the length
  // first rejects most values without mapping them. Null for other 'inner'
  // filters. Shared by copies.
  const std::shared_ptr<const StringLength> lengthFilter_;
  // Valid UTF-8 strings that map to each value of an IN-list 'inner', which
  // lets testBytesRange skip row groups by their min and max. Null if 'inner'
  // is not an IN-list or has a value that is not valid UTF-8. Shared by copies.
  const std::shared_ptr<const std::vector<Preimages>> valuePreimages_;
};

/// Passes strings that contain 'substring' as a sequence of bytes. Used to
/// push down strpos(x, 'abc') > 0 and x LIKE '%abc%', which search bytes the
/// same way, also in invalid UTF-8. A CaseMapped filter around it pushes down
/// strpos(lower(x), 'abc') > 0.
class StringContains final : public Filter {
 public:
  /// @param substring Bytes to find. Must not be empty.
  /// @param nullAllowed Whether nulls pass.
  StringContains(std::string substring, bool nullAllowed);

  folly::dynamic serialize() const override;

  static std::unique_ptr<Filter> create(const folly::dynamic& obj);

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    return std::make_unique<StringContains>(
        substring_, nullAllowed.value_or(nullAllowed_));
  }

  bool testNonNull() const final {
    return true;
  }

  bool hasTestLength() const final {
    return true;
  }

  /// Strings shorter than 'substring' do not contain it.
  bool testLength(int32_t length) const final {
    return length >= static_cast<int64_t>(substring_.size());
  }

  bool testBytes(const char* value, int32_t length) const final;

  /// The substring can be anywhere in a string, so min and max only rule out
  /// a range that has a single value.
  bool testBytesRange(
      std::optional<std::string_view> min,
      std::optional<std::string_view> max,
      bool hasNull) const final;

  /// Merges with filters that this filter implies or that imply it, e.g.
  /// contains 'not set' with contains 'set', and with IN-lists.
  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

  const std::string& substring() const {
    return substring_;
  }

  std::string toString() const final;

  bool testingEquals(const Filter& other) const final;

 private:
  const std::string substring_;
};

} // namespace facebook::velox::common
