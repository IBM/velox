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

#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "velox/expression/Expr.h"
#include "velox/expression/FunctionMetadata.h"
#include "velox/expression/VectorFunctionListener.h"

namespace facebook::velox {
namespace core {
class ExecCtx;
} // namespace core
namespace exec {

class FieldReference;
class VectorFunction;

/// Immutable expression-tree node used by the V2 evaluator.
///
/// All fields are set at construction (from a compiled Expr via
/// ExprV2::from()) and never mutated during evaluation.  Safe to share
/// across threads.
///
/// Per-call evaluation state lives on EvalFrame.  Per-Expr cross-call
/// state (memo, shared-subexpr cache, stats) lives on ExprRuntimeState,
/// owned by ExprSetV2.
///
/// During the migration period, every ExprV2 holds a shared_ptr to the
/// V1 Expr it was adapted from.  For special-form nodes, evaluation
/// delegates to that Expr's evalSpecialForm().  For function-call nodes,
/// the V1 Expr is kept alive only to ensure raw FieldReference* pointers
/// in distinctFields_ remain valid.
///
/// TODO: this wrapper class is transitional.  It carries no per-node
/// data the V1 Expr doesn't already expose; the actual V2 win
/// (immutable nodes + side runtime-state tree) doesn't require a
/// separate node type.  Once V1 is deleted, either rename ExprV2 ->
/// Expr (dropping the old Expr) or eliminate ExprV2 outright and have
/// the V2 evaluator work on Expr& directly.  See ExprV2::from in
/// ExprV2.cpp for the matching note.
class ExprV2 {
 public:
  /// Builds a V2 tree from a compiled V1 Expr.  Recursively converts
  /// children.  For kCast nodes, substitutes a freshly-built
  /// CastExprV2 (using the V2 cast hooks factory) for V1's CastExpr,
  /// so V2 cast evaluation goes through CastExprV2 without modifying
  /// the V1 cast factory.  'execCtx' provides the QueryConfig used by
  /// the hooks factory.
  static std::shared_ptr<ExprV2> from(
      const std::shared_ptr<Expr>& expr,
      core::ExecCtx& execCtx);

  const TypePtr& type() const {
    return type_;
  }

  const std::string& name() const {
    return name_;
  }

  const std::vector<std::shared_ptr<ExprV2>>& inputs() const {
    return inputs_;
  }

  /// Returns null for special-form nodes.  Function-call nodes return
  /// the VectorFunction to invoke during applyFunction().
  const std::shared_ptr<VectorFunction>& vectorFunction() const {
    return vectorFunction_;
  }

  const VectorFunctionMetadata& metadata() const {
    return metadata_;
  }

  const std::vector<VectorFunctionListeners>& listeners() const {
    return listeners_;
  }

  bool isSpecialForm() const {
    return specialFormKind_.has_value();
  }

  std::optional<SpecialFormKind> specialFormKind() const {
    return specialFormKind_;
  }

  /// True if this and all children are deterministic.
  bool deterministic() const {
    return deterministic_;
  }

  /// True if this Expr tree is null for a null in any of the columns
  /// this depends on.
  bool propagatesNulls() const {
    return propagatesNulls_;
  }

  bool supportsFlatNoNullsFastPath() const {
    return supportsFlatNoNullsFastPath_;
  }

  /// True if this or a sub-expression is an IF, AND or OR.
  bool hasConditionals() const {
    return hasConditionals_;
  }

  /// True when this node has the same distinctFields as its parent and
  /// is not multiply-referenced — peeling and null-pruning that would
  /// have been performed identically by the parent are redundant here.
  bool skipFieldDependentOptimizations() const {
    return skipFieldDependentOptimizations_;
  }

  /// True if this expression appears in more than one place in the
  /// containing ExprSet (CSE candidate).  Set during compilation of
  /// the source Expr; mirrored here for the SharedSubexprCache phase.
  bool isMultiplyReferenced() const {
    return isMultiplyReferenced_;
  }

  bool trackCpuUsage() const {
    return trackCpuUsage_;
  }

  /// The distinct references to input columns in this node's
  /// 'inputs_' subtrees.  Empty if this is the same as the parent
  /// Expr's distinctFields.
  const std::vector<FieldReference*>& distinctFields() const {
    return distinctFields_;
  }

  /// Fields referenced by multiple inputs.  A subset of
  /// distinctFields().  Used to determine pre-loading of lazy vectors
  /// at the current Expr.
  const std::unordered_set<FieldReference*>& multiplyReferencedFields() const {
    return multiplyReferencedFields_;
  }

  /// The V1 Expr this node was adapted from.  Always non-null.  Kept
  /// alive to ensure raw FieldReference* pointers remain valid and to
  /// support delegated evaluation of special forms during the migration.
  const std::shared_ptr<Expr>& sourceExpr() const {
    return sourceExpr_;
  }

 private:
  ExprV2() = default;

  TypePtr type_;
  std::string name_;
  std::vector<std::shared_ptr<ExprV2>> inputs_;

  // Null for special-form nodes.
  std::shared_ptr<VectorFunction> vectorFunction_;
  VectorFunctionMetadata metadata_;
  std::vector<VectorFunctionListeners> listeners_;

  std::optional<SpecialFormKind> specialFormKind_;

  // Compile-time metadata copied from sourceExpr_ at construction.

  // True if this and all children are deterministic.
  bool deterministic_{true};

  // True if a null in any of distinctFields_ causes this to be null
  // for the row.
  bool propagatesNulls_{false};

  // Set at compile time based on whether the function's signature and
  // input types make the FlatNoNulls fast path safe.
  bool supportsFlatNoNullsFastPath_{false};

  // True if this or a sub-expression is an IF, AND or OR.
  bool hasConditionals_{false};

  // True when this node has the same distinctFields as its parent
  // and is not multiply-referenced: peeling and null-pruning that
  // would have been performed identically by the parent are
  // redundant here.
  bool skipFieldDependentOptimizations_{false};

  // True if this expression appears more than once in the containing
  // ExprSet.  Drives CSE caching.
  bool isMultiplyReferenced_{false};

  // True if this expression should always track CPU usage (set at
  // compile time from query config).  Distinct from adaptive
  // sampling, which is decided at runtime.
  bool trackCpuUsage_{false};

  // Raw pointers into the V1 tree owned by sourceExpr_.  Valid as long
  // as sourceExpr_ is held.
  std::vector<FieldReference*> distinctFields_;
  std::unordered_set<FieldReference*> multiplyReferencedFields_;

  // Owning reference to the V1 Expr this V2 node was built from.  Kept
  // alive for the lifetime of this ExprV2.
  std::shared_ptr<Expr> sourceExpr_;

  friend class ExprV2Builder;
};

} // namespace exec
} // namespace facebook::velox
