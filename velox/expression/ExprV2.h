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

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "velox/expression/Expr.h"
#include "velox/expression/FunctionMetadata.h"
#include "velox/expression/VectorFunctionListener.h"

namespace facebook::velox::exec {

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
class ExprV2 {
 public:
  /// Builds a V2 tree from a compiled V1 Expr.  Recursively converts
  /// children.
  static std::shared_ptr<ExprV2> from(const std::shared_ptr<Expr>& expr);

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

  bool deterministic() const {
    return deterministic_;
  }

  bool propagatesNulls() const {
    return propagatesNulls_;
  }

  bool supportsFlatNoNullsFastPath() const {
    return supportsFlatNoNullsFastPath_;
  }

  bool hasConditionals() const {
    return hasConditionals_;
  }

  bool trackCpuUsage() const {
    return trackCpuUsage_;
  }

  const std::vector<FieldReference*>& distinctFields() const {
    return distinctFields_;
  }

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
  bool deterministic_{true};
  bool propagatesNulls_{false};
  bool supportsFlatNoNullsFastPath_{false};
  bool hasConditionals_{false};
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

} // namespace facebook::velox::exec
