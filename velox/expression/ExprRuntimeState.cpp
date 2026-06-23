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

#include "velox/expression/ExprRuntimeState.h"

#include "velox/common/base/Exceptions.h"
#include "velox/expression/ExprV2.h"

namespace facebook::velox::exec {

namespace {

void collect(
    const ExprV2& node,
    folly::F14FastMap<const ExprV2*, size_t>& indexByNode) {
  if (indexByNode.contains(&node)) {
    return;
  }
  indexByNode.emplace(&node, indexByNode.size());
  for (const auto& child : node.inputs()) {
    if (child != nullptr) {
      collect(*child, indexByNode);
    }
  }
}

} // namespace

ExprRuntimeStateTree::ExprRuntimeStateTree(
    const std::vector<std::shared_ptr<ExprV2>>& roots) {
  for (const auto& root : roots) {
    if (root != nullptr) {
      collect(*root, indexByNode_);
    }
  }
  states_.resize(indexByNode_.size());
}

ExprRuntimeState& ExprRuntimeStateTree::at(const ExprV2& node) {
  auto it = indexByNode_.find(&node);
  VELOX_CHECK(
      it != indexByNode_.end(),
      "ExprV2 node not in this ExprRuntimeStateTree");
  return states_[it->second];
}

} // namespace facebook::velox::exec
