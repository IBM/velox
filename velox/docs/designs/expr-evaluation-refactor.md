# Expression Evaluation Refactor (ExprV2)

*2026-06-09*

## Summary

`Expr.cpp` (~2500 lines) and `Expr.h` (~1100 lines) mix at least nine
concerns: tree-shape data, immutable metadata, per-call evaluation state,
per-Expr cross-call caches, stats, adaptive sampling, evaluator algorithm,
tracer wiring, and `ExprSet` lifecycle.  The implicit pipeline
(`eval -> evalEncodings -> evalWithMemo -> evalWithNulls -> evalAll ->
evalAllImpl -> ...`) is hard to read because each method is named by what
its predecessor did *not* do.

This document proposes an additive refactor that introduces four new types
in new files and leaves the existing `Expr` class essentially intact:

| New type             | Role                                                    |
|----------------------|---------------------------------------------------------|
| `ExprV2`             | Immutable expression-tree node.                         |
| `ExprRuntimeState`   | Per-node mutable state (memo, shared cache, stats).     |
| `EvalFrame`          | Per-call state, lives on the stack of `evaluate()`.     |
| `ExprEvaluatorV2`    | Stateless orchestrator that drives the staged pipeline. |
| `ExprSetV2`          | Owner of `ExprV2` roots, runtime states, and evaluator. |

The refactor is **behavior-preserving**.  V2 ships behind a query-config
flag, runs alongside V1, and becomes the default only after vector-for-vector
parity is verified across the full expression test suite.  Once parity is
established, the V1 code path can be deleted in a separate change.

## Motivation

### Concrete pain points in `Expr.cpp`

| Site                            | Concern                                                 |
|---------------------------------|---------------------------------------------------------|
| `Expr::eval` (816)              | Fast path, exception scope, empty rows, lazy load mixed together. |
| `Expr::evalEncodings` (1101)    | Field peeling.                                          |
| `Expr::evalWithMemo` (1246)     | Dictionary memoization, only reachable on peeled path.  |
| `Expr::evalWithNulls` (1201)    | Null pruning.                                           |
| `Expr::evalAll` (1450)          | Shared subexpression wrapper.                           |
| `Expr::evalAllImpl` (1479)      | Special-form vs function-call fork; arg eval; finalize. |
| `Expr::applyFunctionWithPeeling`(1534) | Argument peeling.                                |
| `Expr::applyFunction` (1753)    | Listener / timing wrapper.                              |
| `Expr::evaluateSharedSubexpr` (899) | Shared-result cache management.                     |

Specific structural problems:

1. **Per-call state lives on the tree node.**  `Expr::inputValues_` is a
   buffer reset every evaluation but stored on the node.  This prevents
   concurrent evaluation of the same `ExprSet` from multiple threads.
2. **Per-Expr cross-call state mixes with tree data.**
   `sharedSubexprResults_`, `baseOfDictionary*`, `cachedDictionaryIndices_`,
   `stats_`, `adaptiveState_` are all on `Expr` alongside `inputs_` /
   `type_` / `vectorFunction_` — making it impossible to tell which fields
   are immutable post-compile.
3. **Implicit pipeline order.**  Layer names (`evalEncodings`,
   `evalWithNulls`, `evalAllImpl`) describe local actions but not pipeline
   position.  A reader must trace calls to know where they are.
4. **Special-form vs function-call fork is buried.**  The check happens at
   `evalAllImpl` line 1486, four call levels below `eval()`.
5. **No documented row-space invariant.**  Code juggles `rows` (caller's
   original), `remainingRows` (post-pruning), translated inner rows
   (post-peeling), and `EvalCtx::finalSelection()` without naming the
   relationship.
6. **`EvalCtx` mutations are inconsistently scoped.**  Some sites use
   `ContextSaver` / `ScopedVarSetter` / `ScopedFinalSelectionSetter`,
   others mutate and restore by hand inline.

### Why not refactor `Expr` in place?

Velox accepts heavy upstream churn on `Expr.cpp`.  An in-place restructure
would generate continuous rebase conflicts for the duration of the
refactor.  A parallel `ExprV2` type in new files isolates the change and
lets upstream evolve `Expr` undisturbed until cutover.

## Goals

- **Behavior preservation.**  Bit-for-bit identical output to V1 across
  the full expression test suite.
- **Readable pipeline.**  One function per phase boundary, each ~5–10
  lines, with documented invariants.
- **Thread-safety.**  Concurrent evaluation of the same `ExprSetV2` from
  multiple threads with separate `EvalFrame`s.  (Today impossible because
  `Expr::inputValues_` is on the node.)
- **Testable phases.**  Each pipeline function callable in isolation by
  constructing an `EvalFrame` and invoking the phase directly.
- **Incremental rollout.**  V2 ships behind a flag; V1 stays the default
  until parity verified; cutover and V1 deletion are separate changes.

## Non-goals

- No new optimizations.  Performance equivalent to V1, not better.
- No changes to `EvalCtx`'s public API.
- No changes to special-form subclasses (`CaseExpr`, `ConjunctExpr`,
  `CastExpr`, `CoalesceExpr`, `FieldReference`, `ConstantExpr`) initially.
  `ExprV2` for a special-form node holds a `shared_ptr<Expr>` and
  delegates `evaluateSpecialForm` to it.  Migrating special forms to
  native ExprV2 nodes is a follow-up.
- No changes to the expression compiler or function registry.  An
  adapter walks the compiled `Expr` tree once and produces an `ExprV2` tree.
- `ExprSetSimplified` is not subsumed initially.  After V2 stabilizes,
  `ExprSetSimplified` is reimplemented as the same pipeline with peeling,
  memo, and shared-subexpr phases disabled.

## Target design

### Component overview

```
                       ExprSetV2  (owner)
                           |
        +------------------+------------------+
        |                  |                  |
   shared_ptr<       ExprRuntime      ExprEvaluatorV2
   ExprV2>[]         StateTree         (stateless)
   (immutable        (mutable,
    tree)            parallel
                     to tree)
```

For each top-level evaluation, `ExprSetV2::eval`:

1. Constructs an `EvalFrame` on the stack referencing the root `ExprV2`,
   its `ExprRuntimeState`, the caller's `EvalCtx`, the requested rows,
   and the output slot.
2. Calls `ExprEvaluatorV2::evaluate(frame, this)`.
3. The evaluator drives the pipeline, recursing by constructing inner
   frames where row-space changes.

### `ExprV2` — immutable node

```cpp
namespace facebook::velox::exec {

/// Immutable expression-tree node.  All fields are set at construction
/// from a compiled Expr; none are mutated during evaluation.  Safe to
/// share across threads.
class ExprV2 {
 public:
  /// Construct from a compiled Expr.  Recursively builds the V2 tree;
  /// special-form nodes wrap the original Expr for delegated evaluation.
  static std::shared_ptr<ExprV2> from(const std::shared_ptr<Expr>& expr);

  const TypePtr& type() const { return type_; }
  const std::string& name() const { return name_; }
  const std::vector<std::shared_ptr<ExprV2>>& inputs() const { return inputs_; }
  const VectorFunction* vectorFunction() const { return vectorFunction_.get(); }
  const VectorFunctionMetadata& metadata() const { return metadata_; }
  SpecialForm specialForm() const { return specialForm_; }

  bool isSpecialForm() const { return specialForm_ != SpecialForm::kNone; }
  bool deterministic() const { return deterministic_; }
  bool propagatesNulls() const { return propagatesNulls_; }
  bool supportsFlatNoNullsFastPath() const { return supportsFlatNoNullsFastPath_; }
  bool hasConditionals() const { return hasConditionals_; }
  bool skipFieldDependentOptimizations() const { return skipFieldDependentOptimizations_; }

  const std::vector<FieldReferenceV2*>& distinctFields() const { return distinctFields_; }
  const std::vector<FieldReferenceV2*>& multiplyReferencedFields() const { return multiplyReferencedFields_; }

  /// Delegated path for special-form nodes during the migration period.
  /// Returns the wrapped Expr; null for function-call nodes.
  const std::shared_ptr<Expr>& legacySpecialForm() const { return legacySpecialForm_; }

 private:
  ExprV2(...);  // populated by from()

  TypePtr type_;
  std::string name_;
  std::vector<std::shared_ptr<ExprV2>> inputs_;
  std::shared_ptr<VectorFunction> vectorFunction_;  // null for special forms
  VectorFunctionMetadata metadata_;
  SpecialForm specialForm_;                          // enum tag
  std::shared_ptr<Expr> legacySpecialForm_;          // delegation during migration

  // Compile-time metadata derived once.
  bool deterministic_;
  bool propagatesNulls_;
  bool supportsFlatNoNullsFastPath_;
  bool hasConditionals_;
  bool skipFieldDependentOptimizations_;

  std::vector<FieldReferenceV2*> distinctFields_;
  std::vector<FieldReferenceV2*> multiplyReferencedFields_;
};

} // namespace facebook::velox::exec
```

### `ExprRuntimeState` — per-node mutable state

```cpp
/// Mutable per-Expr state that survives across evaluations.  One instance
/// per ExprV2 per ExprSetV2.  Owned by ExprSetV2 in a tree that mirrors
/// the ExprV2 tree.  Not thread-safe: concurrent evaluations of the same
/// ExprSetV2 each get their own ExprRuntimeStateTree (or share with a
/// mutex; TBD by use case).
struct ExprRuntimeState {
  // Shared-subexpression cache.  Same as Expr::sharedSubexprResults_.
  SharedSubexprCacheState sharedCache;

  // Dictionary memoization.  Same as Expr's baseOfDictionary*,
  // cachedDictionaryIndices_, dictionaryCache_.
  DictionaryMemoState dictMemo;

  // Stats accumulated across evaluations.
  ExprStats stats;

  // Adaptive CPU sampling state.
  AdaptiveCpuSamplingState adaptiveState;
};

/// Tree of runtime state, parallel-indexed with the ExprV2 tree.
/// Look up runtime state for a node by tree position.
class ExprRuntimeStateTree {
 public:
  explicit ExprRuntimeStateTree(const ExprV2& root);
  ExprRuntimeState& at(const ExprV2& node);
 private:
  // Implementation choice: flat vector keyed by in-order index, or
  // a map keyed by ExprV2*, or a shared_ptr per node.  Trade-off TBD;
  // flat vector is simplest if the tree is built once and never reshaped.
  std::vector<ExprRuntimeState> states_;
  folly::F14FastMap<const ExprV2*, size_t> indexByNode_;
};
```

### `EvalFrame` — per-call state

```cpp
/// Per-call evaluation state for one ExprV2 node.  Lives on the stack
/// of the outermost ExprEvaluatorV2::evaluate() call.  Inner recursion
/// (peeling, null-pruning wrapper, shared-subexpr wrapper) constructs
/// fresh inner EvalFrames.
///
/// Row-space invariant:
///   - originalRows never changes after construction.
///   - remainingRows.rows() is always a subset of originalRows.
///   - Every phase operates on remainingRows.rows(), not originalRows.
///   - remainingRows shrinks monotonically as evaluation proceeds; it
///     never grows.
///   - originalRows is used only for result sizing, final null-fill,
///     and copying / wrapping back into the caller's row space.
struct EvalFrame {
  // === Bindings (constant after construction) ===

  ExprV2& expr;
  ExprRuntimeState& nodeRuntime;
  EvalCtx& ctx;
  const SelectivityVector& originalRows;
  VectorPtr& result;

  // === Mutable state flowing between phases ===

  MutableRemainingRows remainingRows;
  std::vector<VectorPtr> inputValues;  // moved off ExprV2 onto the frame
  bool tryPeelArgs;

  // === Compile-time flags cached once from expr ===
  bool defaultNulls;
  bool propagatesNulls;
  bool deterministic;
  bool isSpecialForm;
  bool supportsFlatNoNullsFastPath;
  bool hasConditionals;
  bool skipFieldDependentOptimizations;

  EvalFrame(
      ExprV2& exprIn,
      ExprRuntimeState& nodeRuntimeIn,
      EvalCtx& ctxIn,
      const SelectivityVector& rowsIn,
      VectorPtr& resultIn);
};
```

### `ExprEvaluatorV2` — stateless orchestrator

```cpp
/// Drives the staged evaluation pipeline against an EvalFrame.  Owns
/// no state; all per-call state lives on the frame, all cross-call
/// state lives in ExprRuntimeState.  Safe to share across threads.
class ExprEvaluatorV2 {
 public:
  /// Entry point.  Constructs no state; orchestrates phases.
  void evaluate(EvalFrame& frame, const ExprSetV2* parentSet);

 private:
  // Pipeline phases.  Order matches V1 semantics exactly.  Each name
  // describes what wrapper or fork this layer owns, not what the
  // previous layer did.
  void evaluateFrame(EvalFrame& f, const ExprSetV2* parentSet);
  void evaluateWithFieldPeeling(EvalFrame& f);
  void evaluateWithNullPruning(EvalFrame& f);
  void evaluateWithSharedSubexpr(EvalFrame& f);
  void evaluateNodeBody(EvalFrame& f);
  void evaluateFunctionCall(EvalFrame& f);
  void evaluateSpecialForm(EvalFrame& f);

  // Apply / leaf operations.
  void applyFunction(EvalFrame& f);
  void emitEmpty(EvalFrame& f);
};
```

### `ExprSetV2` — owner

```cpp
class ExprSetV2 {
 public:
  ExprSetV2(
      std::vector<core::TypedExprPtr> exprs,
      core::ExecCtx* execCtx);

  /// Public entry point.  Constructs an EvalFrame per root and calls
  /// the evaluator.  Threading: each concurrent caller passes its own
  /// EvalCtx; ExprSetV2 either provides per-thread runtime-state trees
  /// or guards a shared one with a mutex (TBD).
  void eval(
      const SelectivityVector& rows,
      EvalCtx& ctx,
      std::vector<VectorPtr>& results);

  void addToMemo(ExprV2* expr);

  const std::vector<std::shared_ptr<ExprV2>>& exprs() const { return roots_; }

 private:
  std::vector<std::shared_ptr<ExprV2>> roots_;
  std::unique_ptr<ExprRuntimeStateTree> runtimeStates_;
  ExprEvaluatorV2 evaluator_;
};
```

### Phases — stateless utility types

Each phase is a stateless type with static methods that take `EvalFrame&`
and (where applicable) a continuation callback.  Free functions in an
anonymous namespace would work equally well; class form is for grouping
and forward declaration.

```cpp
struct FastPath {
  static bool tryFlatNoNulls(EvalFrame& f, const ExprSetV2* parentSet);
};

struct LazyInput {
  static void loadRequiredFields(EvalFrame& f);
};

struct FieldPeeling {
  /// Attempts to peel input field encodings for the whole subtree.
  /// If peeling succeeds, calls continuation on a fresh inner frame
  /// with translated rows; wraps the result back into the outer row
  /// space.  Internally chooses DictionaryMemo::evaluate vs direct
  /// continuation based on the peel result's mayCache flag.
  template <typename Continuation>
  static bool tryPeel(EvalFrame& f, Continuation continuation);
};

struct DictionaryMemo {
  /// Reached only on the peeled, mayCache=true path.  Caches results
  /// keyed by base dictionary and re-uses across batches.
  template <typename Continuation>
  static void evaluate(EvalFrame& f, Continuation continuation);
};

struct NullPruning {
  /// Wrapping phase.  If propagatesNulls and inputs may have nulls,
  /// computes the non-null subset of remainingRows, constructs an
  /// inner frame on that subset, calls continuation, then null-fills
  /// the pruned rows in the result on return.
  template <typename Continuation>
  static bool tryPrune(EvalFrame& f, Continuation continuation);
};

struct SharedSubexprCache {
  /// Wrapping phase.  Checks nodeRuntime.sharedCache for a cached
  /// result on the same input fields.  On hit, copies into result
  /// and returns true.  On miss, calls continuation and stores
  /// the result.
  template <typename Continuation>
  static bool tryReuse(EvalFrame& f, Continuation continuation);
};

struct ArgEval {
  /// Evaluates all child inputs, populating frame.inputValues.  May
  /// shrink frame.remainingRows in place when default-null behavior
  /// applies.  Returns false if remainingRows becomes empty (in which
  /// case setAllNulls has already been applied).
  static bool evaluate(EvalFrame& f, ExprEvaluatorV2& evaluator);
};

struct ArgPeeling {
  /// Argument-level peeling: peels the encodings of frame.inputValues
  /// after child evaluation.  Distinct from FieldPeeling, which peels
  /// input field encodings before children evaluate.
  template <typename ApplyFn>
  static bool tryApply(EvalFrame& f, ApplyFn apply);
};
```

### `ArgEvalStrategy` — the one true policy

```cpp
/// Strategy for evaluating child inputs.  Two implementations:
///   DefaultNullArgEval     — prune rows where any child is null.
///   PreserveNullArgEval    — leave nulls in place for the function
///                            to handle.
/// Selected per-frame from expr.metadata().defaultNullBehavior.
class ArgEvalStrategy {
 public:
  virtual ~ArgEvalStrategy() = default;

  /// Returns true if remainingRows still has selections after
  /// evaluation.  False means setAllNulls has been applied.
  virtual bool evalArgs(EvalFrame& f, ExprEvaluatorV2& evaluator) = 0;
};

class DefaultNullArgEval : public ArgEvalStrategy { /* ... */ };
class PreserveNullArgEval : public ArgEvalStrategy { /* ... */ };
```

Selected once per frame at frame construction:
```cpp
ArgEvalStrategy& strategy = f.defaultNulls
    ? defaultNullStrategy_
    : preserveNullStrategy_;
```

Strategies are shared (stateless), not allocated per call.

### Debug guards

Two RAII helpers verify invariants in debug builds.  Both compile to
zero-cost no-ops in release.

```cpp
#ifndef NDEBUG
/// Verifies the row-space invariant at every phase boundary:
///   - remainingRows is a subset of originalRows.
///   - remainingRows shrinks monotonically (never grows during phase).
class DebugRemainingRowsGuard {
 public:
  explicit DebugRemainingRowsGuard(const EvalFrame& frame)
      : frame_{frame},
        countOnEntry_{frame.remainingRows.rows().countSelected()} {}

  ~DebugRemainingRowsGuard() {
    VELOX_DCHECK(frame_.remainingRows.rows().isSubset(frame_.originalRows));
    VELOX_DCHECK_LE(
        frame_.remainingRows.rows().countSelected(), countOnEntry_);
  }

 private:
  const EvalFrame& frame_;
  vector_size_t countOnEntry_;
};

/// Installed once at the top of evaluateFrame().  Verifies invariants
/// that hold across the entire evaluation, not at each phase boundary.
class DebugEvaluateGuard {
 public:
  explicit DebugEvaluateGuard(const EvalFrame& frame) : frame_{frame} {
    VELOX_DCHECK(frame.inputValues.empty());
  }

  ~DebugEvaluateGuard() {
    VELOX_DCHECK(frame_.inputValues.empty()); // releaseGuard ran
    if (frame_.result) {
      VELOX_DCHECK(frame_.result->type()->equivalent(*frame_.expr.type()));
      VELOX_DCHECK_GE(frame_.result->size(), frame_.originalRows.end());
    }
  }

 private:
  const EvalFrame& frame_;
};
#else
class DebugRemainingRowsGuard { public: explicit DebugRemainingRowsGuard(const EvalFrame&) {} };
class DebugEvaluateGuard { public: explicit DebugEvaluateGuard(const EvalFrame&) {} };
#endif
```

## Pipeline

Order matches V1 semantics exactly (see `Expr.cpp` reference column).

```cpp
void ExprEvaluatorV2::evaluate(EvalFrame& f, const ExprSetV2* parentSet) {
  evaluateFrame(f, parentSet);
}

// Entry guards.  Mirrors Expr::eval (816).
void ExprEvaluatorV2::evaluateFrame(EvalFrame& f, const ExprSetV2* parentSet) {
  DebugEvaluateGuard outerGuard{f};

  if (FastPath::tryFlatNoNulls(f, parentSet)) return;

  TopLevelExceptionScope scope{f, parentSet};

  if (!f.originalRows.hasSelections()) {
    emitEmpty(f);
    return;
  }

  LazyInput::loadRequiredFields(f);
  evaluateWithFieldPeeling(f);
}

// Field-peeling wrapper.  Mirrors Expr::evalEncodings (1101).
void ExprEvaluatorV2::evaluateWithFieldPeeling(EvalFrame& f) {
  DebugRemainingRowsGuard guard{f};
  if (FieldPeeling::tryPeel(f, [this](EvalFrame& inner) {
        evaluateWithNullPruning(inner);
      })) {
    return;
  }
  evaluateWithNullPruning(f);
}

// Null-pruning wrapper.  Mirrors Expr::evalWithNulls (1201).
void ExprEvaluatorV2::evaluateWithNullPruning(EvalFrame& f) {
  DebugRemainingRowsGuard guard{f};
  if (NullPruning::tryPrune(f, [this](EvalFrame& pruned) {
        evaluateWithSharedSubexpr(pruned);
      })) {
    return;
  }
  evaluateWithSharedSubexpr(f);
}

// Shared-subexpr wrapper.  Mirrors Expr::evalAll (1450).
void ExprEvaluatorV2::evaluateWithSharedSubexpr(EvalFrame& f) {
  DebugRemainingRowsGuard guard{f};
  if (SharedSubexprCache::tryReuse(f, [this](EvalFrame& inner) {
        evaluateNodeBody(inner);
      })) {
    return;
  }
  evaluateNodeBody(f);
}

// Fork.  Mirrors the isSpecialForm() check in Expr::evalAllImpl (1486).
void ExprEvaluatorV2::evaluateNodeBody(EvalFrame& f) {
  if (f.isSpecialForm) {
    evaluateSpecialForm(f);
    return;
  }
  evaluateFunctionCall(f);
}

// Function-call leaf.  Mirrors the rest of evalAllImpl + applyFunction*.
void ExprEvaluatorV2::evaluateFunctionCall(EvalFrame& f) {
  DebugRemainingRowsGuard guard{f};
  auto releaseGuard = folly::makeGuard([&] {
    f.ctx.releaseVectors(f.inputValues);
    f.inputValues.clear();
  });

  if (!ArgEval::evaluate(f, *this)) {
    return; // setAllNulls already applied
  }

  if (!f.tryPeelArgs ||
      !ArgPeeling::tryApply(f, [this](EvalFrame& g) { applyFunction(g); })) {
    applyFunction(f);
  }

  if (f.remainingRows.hasChanged()) {
    EvalCtx::addNulls(
        f.originalRows,
        f.remainingRows.rows().asRange().bits(),
        f.ctx,
        f.expr.type(),
        f.result);
  }
}

// Special-form leaf.  Delegates to wrapped legacy Expr during migration.
void ExprEvaluatorV2::evaluateSpecialForm(EvalFrame& f) {
  DebugRemainingRowsGuard guard{f};
  f.expr.legacySpecialForm()->evalSpecialForm(
      f.remainingRows.rows(), f.ctx, f.result);
}
```

### Phase-to-V1 mapping

| V2 phase                       | V1 site                              | Responsibility                       |
|--------------------------------|--------------------------------------|--------------------------------------|
| `evaluateFrame`                | `Expr::eval` (816)                   | Fast path, exception scope, empty rows, lazy load. |
| `evaluateWithFieldPeeling`     | `Expr::evalEncodings` (1101)         | Field peeling wrapper.               |
| `FieldPeeling::tryPeel`        | `peelEncodings` (1025)               | Compute peel, recurse on inner rows. |
| `DictionaryMemo::evaluate`     | `Expr::evalWithMemo` (1246)          | Cache by base dictionary.            |
| `evaluateWithNullPruning`      | `Expr::evalWithNulls` (1201)         | Null pruning wrapper.                |
| `evaluateWithSharedSubexpr`    | `Expr::evalAll` (1450)               | Shared-subexpr wrapper.              |
| `SharedSubexprCache::tryReuse` | `Expr::evaluateSharedSubexpr` (899)  | Cache lookup + partial-row eval.     |
| `evaluateNodeBody`             | `Expr::evalAllImpl` (1486) fork      | Special-form vs function-call fork.  |
| `evaluateFunctionCall`         | `Expr::evalAllImpl` (1490–1531) tail | Arg eval, arg peeling, apply, null fill. |
| `ArgEval::evaluate`            | `evalArgsDefaultNulls` (380) / `evalArgsWithNulls` (455) | Evaluate children. |
| `ArgPeeling::tryApply`         | `applyFunctionWithPeeling` (1534)    | Argument-level peeling.              |
| `applyFunction`                | `Expr::applyFunction` (1753)         | Invoke vector function + listeners.  |

## UML diagrams

### Class diagram

```
  Legend:  <>-- composition (owns lifetime)
           o-- aggregation (holds reference)
           --> uses / depends on
           --|> implements interface
           « »  stereotype


  +------------------------------------------------------------------+
  |                          ExprSetV2                               |
  |  «owner»                                                         |
  +------------------------------------------------------------------+
  | - roots_     : vector<shared_ptr<ExprV2>>                        |
  | - runtimeStates_ : unique_ptr<ExprRuntimeStateTree>              |
  | - evaluator_ : ExprEvaluatorV2                                   |
  +------------------------------------------------------------------+
  | + eval(rows, ctx, results[])                                     |
  | + addToMemo(ExprV2*)                                             |
  +--------+-----------------+---------------------+-----------------+
           <>                <>                    <>
           |                 |                     |
           v                 v                     v
  +--------------------+ +-----------------+  +---------------------------+
  |      ExprV2        | | ExprRuntimeStateTree |  ExprEvaluatorV2        |
  | «immutable node»   | | «owner of nodes»|  |     «stateless»          |
  +--------------------+ +-----------------+  +---------------------------+
  | - type_            | | - states_       |  | + evaluate(frame,        |
  | - name_            | | - indexByNode_  |  |     parentSet)           |
  | - inputs_          | +-----------------+  | - evaluateFrame(f, ps)   |
  | - vectorFunc_      |         <>           | - evaluateWithField...   |
  | - metadata_        |         |            | - evaluateWithNull...    |
  | - specialForm_     |         v            | - evaluateWithShared...  |
  | - legacySpecial_   | +-----------------+  | - evaluateNodeBody       |
  | - distinctFields_  | | ExprRuntimeState|  | - evaluateFunctionCall   |
  +-----+--------------+ |                 |  | - evaluateSpecialForm    |
        <>               | - sharedCache   |  | - applyFunction          |
        | inputs_        | - dictMemo      |  +-----------+--------------+
        |                | - stats         |              | uses
        v                | - adaptiveState |              v
  +--------------------+ +-----------------+   +---------------------------+
  |      ExprV2        |                       |        «phases»           |
  +--------------------+                       | (stateless types)         |
                                               +---------------------------+
                                               | FastPath                  |
                                               | LazyInput                 |
                                               | FieldPeeling              |
                                               | DictionaryMemo            |
                                               | NullPruning               |
                                               | SharedSubexprCache        |
                                               | ArgEval -+                |
                                               | ArgPeeling                |
                                               +--+----+--+----------------+
                                                  | takes &f  | uses
                                                  v           v
                                       +----------------+ +------------------+
                                       |   EvalFrame    | | ArgEvalStrategy  |
                                       | «per-call»     | | «interface»      |
                                       +----------------+ +--------+---------+
                                       | & expr         |        --|--
                                       | & nodeRuntime  |       /     \
                                       | & ctx          |      v       v
                                       | & originalRows |  +-------+ +---------+
                                       | & result       |  |Default| |Preserve |
                                       |   remainingRows|  |Null   | |Null     |
                                       |   inputValues  |  |ArgEval| |ArgEval  |
                                       |   tryPeelArgs  |  +-------+ +---------+
                                       |   [flags]      |
                                       +----+----+------+
                                            o    o
                                            |    |
                                            v    v
                            +----------------+ +-----------------+
                            |ExprRuntimeState| |    EvalCtx      |
                            +----------------+ | «existing»      |
                                               +-----------------+
```

### Sequence diagram for one `evaluate()` call

```
caller        ExprSetV2     ExprEvaluatorV2     phases         EvalFrame      EvalCtx
  |              |                |                |              (stack)         |
  | eval(rows,   |                |                |                |             |
  |   ctx,res[]) |                |                |                |             |
  |------------->|                |                |                |             |
  |              | construct      |                |                |             |
  |              | EvalFrame------+--------------->|                |             |
  |              |                |                |                |             |
  |              | evaluate(f, this)               |                |             |
  |              |--------------->|                |                |             |
  |              |                |                |                |             |
  |              |                | FastPath::tryFlatNoNulls(f)     |             |
  |              |                |--------------->| read flags     |             |
  |              |                |                |<---------------|             |
  |              |                | false                           |             |
  |              |                |<---------------|                |             |
  |              |                |                |                |             |
  |              |                | TopLevelExceptionScope installed              |
  |              |                |-----------------------------------------------|
  |              |                |                |                |             |
  |              |                | LazyInput::loadRequiredFields(f)|             |
  |              |                |--------------->| ensureFieldLoaded            |
  |              |                |                |--------------+-------------->|
  |              |                |                |                |             |
  |              |                | FieldPeeling::tryPeel(f, &evaluateWith...)    |
  |              |                |--------------->| saveAndReset, setPeeled      |
  |              |                |                |-----------------------------> |
  |              |                |                | construct innerFrame         |
  |              |                |                |--------------> (new frame)   |
  |              |                |                | recurse(innerFrame)          |
  |              |                |<---------------|                |             |
  |              |                | ... NullPruning, SharedSubexprCache,          |
  |              |                |     fork, ArgEval, ArgPeeling, apply ...      |
  |              |                |                | wrap peeled result           |
  |              |                |                | moveOrCopyResult to outer    |
  |              |                | true                            |             |
  |              |                |<---------------|                |             |
  |              | (early return; releaseGuard cleared inputValues)               |
  |              | result[i] populated             |                |             |
  |<-------------|                |                |                |             |
```

### State diagram for `remainingRows`

```
                  remainingRows == originalRows
                          (initial)
                              |
                              | enter evaluateFrame
                              v
        +-----------------+---+---------------+---------------+
        |                 |                   |               |
   FieldPeeling      NullPruning         SharedSubexpr    (no shrink)
   (no shrink         (no shrink         (no shrink
    on outer;          on outer;          on outer;
    inner frame        inner frame        inner frame
    has its own        has its own        has its own
    originalRows)      originalRows)      originalRows)
        |                 |                   |               |
        +-----------------+---+---------------+---------------+
                              |
                              v
                       evaluateNodeBody
                              |
                +-------------+-------------+
                |                           |
          evaluateSpecialForm        evaluateFunctionCall
          (no shrink)                       |
                                            v
                                       ArgEval::evaluate
                                       (may shrink in place
                                        via default-null or
                                        error deselection)
                                            |
                                            v
                                       remainingRows
                                       <= original
                                            |
                                            v
                                       ArgPeeling / applyFunction
                                       (operate on
                                        remainingRows.rows())
                                            |
                                            v
                                       if shrank, addNulls for
                                       originalRows \ remainingRows
```

## Recursion patterns

Two distinct shapes; each phase uses exactly one.

**Pattern A — wrapping recursion** (FieldPeeling, NullPruning, SharedSubexprCache):
- Phase constructs a new `EvalFrame` with a different `originalRows`
  (peeled inner rows, or pruned subset).
- The outer frame's `remainingRows` is not mutated by the phase.
- The inner frame has its own invariants; the guard on the outer
  frame sees no change.

**Pattern B — in-place mutation** (ArgEval):
- Same frame; `remainingRows` shrinks in place as children evaluate
  and null/error rows are deselected.
- The guard on the frame sees the shrinkage and verifies subset +
  monotonicity.

No phase mixes both patterns.

## Adapter from `Expr` to `ExprV2`

```cpp
// static
std::shared_ptr<ExprV2> ExprV2::from(const std::shared_ptr<Expr>& expr) {
  // Recursively convert children.
  std::vector<std::shared_ptr<ExprV2>> inputs;
  inputs.reserve(expr->inputs().size());
  for (const auto& child : expr->inputs()) {
    inputs.push_back(ExprV2::from(child));
  }

  // For special forms, wrap the legacy Expr and delegate.
  if (expr->isSpecialForm()) {
    return std::shared_ptr<ExprV2>(new ExprV2(
        expr->type(),
        expr->name(),
        std::move(inputs),
        /*vectorFunction=*/nullptr,
        expr->vectorFunctionMetadata(),
        toSpecialFormTag(expr),
        /*legacySpecialForm=*/expr,
        /* ...flags from expr... */));
  }

  // Function-call node: full ExprV2.
  return std::shared_ptr<ExprV2>(new ExprV2(
      expr->type(),
      expr->name(),
      std::move(inputs),
      expr->vectorFunction(),
      expr->vectorFunctionMetadata(),
      SpecialForm::kNone,
      /*legacySpecialForm=*/nullptr,
      /* ...flags from expr... */));
}
```

This preserves all parsing, typing, type-coercion, function-lookup, and
metadata-derivation logic in the existing `ExprCompiler`.  Only the
evaluation half is re-implemented.

## Implementation steps

Each step is a behavior-preserving PR, reviewed and merged independently.
V2 is feature-flagged off until step 11.

### Step 1 — design doc + edge-case tests

Land this design doc.  Add tests in
`velox/expression/tests/ExprPipelineEdgeCasesTest.cpp` that lock in
current behavior for the subtle cases the refactor must preserve:

- TRY × shared-subexpr partial-row reuse.
- TRY with failing children under default-null vs preserve-null.
- Dictionary peeling with nulls in the indices.
- Dictionary memo with `finalSelection` set.
- Shared-subexpr with multiple input fields, partial-row hit.
- Lazy vectors under IF / AND / OR with `hasConditionals`.
- Flat-no-nulls fast path compatibility with each operator.
- `evalSimplified` equivalence on a representative query.

These tests run against V1 first to capture the current expected output
as golden vectors; they will later run against V2 to confirm parity.

### Step 2 — skeletons of all V2 types

Add empty headers and minimal implementations for `ExprV2`,
`ExprRuntimeState`, `ExprRuntimeStateTree`, `EvalFrame`,
`ExprEvaluatorV2`, `ExprSetV2`.  Nothing routes through them yet.
Compiles, does not link into any executable path.

### Step 3 — adapter `ExprV2::from(Expr)` for function-call nodes

Implement the adapter.  For special-form nodes, wrap legacy `Expr` and
mark with `SpecialForm` tag.  Add unit tests that compile a query,
adapt to V2, and verify metadata round-trips.

### Step 4 — trivial pipeline paths

Implement `ExprEvaluatorV2::evaluate` for:
- FastPath (delegate to existing `evalFlatNoNulls` logic in V1; keep
  bit-identical).
- Empty rows.
- Special-form delegation (`evaluateSpecialForm` calls
  `legacySpecialForm_->evalSpecialForm`).
- Function-call without peeling, memo, or shared-subexpr: arg eval +
  apply.

At this point, V2 can evaluate simple queries.  Add an A/B harness in
test code that evaluates a query with both V1 and V2 and asserts vector
equality.

### Step 5 — FieldPeeling phase

Port `evalEncodings` / `peelEncodings` logic into `FieldPeeling::tryPeel`.
Construct an inner `EvalFrame` for the peeled row space.  Verify the
A/B harness passes for peeling-eligible queries.

### Step 6 — DictionaryMemo phase

Port `evalWithMemo` into `DictionaryMemo::evaluate`.  Reachable only
from `FieldPeeling::tryPeel` when the peel result is cacheable.

### Step 7 — NullPruning phase

Port `evalWithNulls`'s null-pruning branch into `NullPruning::tryPrune`.
Wrapping recursion constructs an inner frame on the pruned row space.

### Step 8 — SharedSubexprCache phase

Port `evaluateSharedSubexpr` into `SharedSubexprCache::tryReuse`.
This is the seam most likely to surface subtle TRY-interaction bugs;
run the step-1 edge-case tests carefully.

### Step 9 — ArgEval + ArgEvalStrategy

Port `evalArgsDefaultNulls` and `evalArgsWithNulls` into
`DefaultNullArgEval::evalArgs` and `PreserveNullArgEval::evalArgs`.

### Step 10 — ArgPeeling + listener / timing / tracer

Port `applyFunctionWithPeeling`, `invokeApplyWithListeners`, the
adaptive CPU-sampling logic, and tracer hooks.

### Step 11 — feature flag + A/B in production tests

Add a query-config flag `expr_eval_v2`.  When set, `ExprSetV2` runs;
otherwise V1.  Run the full `velox_expression_test` suite under both
flags.  Compare results vector-for-vector for every test case.

### Step 12 — migrate special forms

For each of `CaseExpr`, `ConjunctExpr`, `CastExpr`, `CoalesceExpr`,
`FieldReference`, `ConstantExpr`: produce a native `ExprV2` form and
remove the delegation wrapper.  One PR per form.  Order suggested:
`FieldReference` and `ConstantExpr` first (simplest), then `CastExpr`,
then conditionals.

### Step 13 — subsume `ExprSetSimplified`

Reimplement `ExprSetSimplified` as `ExprSetV2` with peeling, memo, and
shared-subexpr phases disabled.  Delete `evalSimplified` and
`evalSimplifiedImpl` from V1.

### Step 14 — cutover

Flip the `expr_eval_v2` default to true after one release cycle of
no parity bugs in production.

### Step 15 — delete V1

Once the flag has been default-true for one release cycle and no parity
flag is held by any caller, delete `Expr::eval` and the chain of
methods it calls.  Keep `Expr` as a compile-time tree node consumed by
`ExprV2::from()`, or merge `Expr` and `ExprV2` if it makes sense.

## Milestones

| Milestone | Steps | Outcome |
|-----------|-------|---------|
| **M1: Foundations**          | 1–3   | Design locked, edge cases captured as golden tests, V2 types compile, adapter works. |
| **M2: Trivial pipeline**     | 4     | V2 can evaluate simple function-call queries; A/B harness in test code passes for trivial cases. |
| **M3: Encoding paths**       | 5–6   | Field peeling and dictionary memo working in V2; A/B parity for peeling-heavy queries. |
| **M4: Null + cache paths**   | 7–8   | Null pruning and shared-subexpr working in V2; A/B parity for TRY / propagatesNulls queries. |
| **M5: Function-call leaf**   | 9–10  | Arg eval (both strategies), arg peeling, listeners, tracer working; V2 reaches full feature parity for function-call nodes. |
| **M6: Production rollout**   | 11    | `expr_eval_v2` flag in place; full test suite passes under V2; opt-in available in production. |
| **M7: Special-form native**  | 12    | Each special form runs natively under V2 instead of delegating; legacy `Expr` no longer reached on V2 path. |
| **M8: Simplified subsumed**  | 13    | `ExprSetSimplified` deleted; V2 covers both paths. |
| **M9: Cutover**              | 14    | V2 is the default in production. |
| **M10: V1 deleted**          | 15    | Legacy `Expr::eval` chain removed; codebase has one evaluator. |

Each milestone is releasable independently.  No milestone (except M10)
removes existing functionality.

## Testing strategy

### Golden-output tests

For each of the step-1 edge cases, capture V1's output as a golden vector.
After each subsequent step, run the same test against both V1 and V2 and
assert byte-equal vectors (including nulls, encodings, and child vector
shapes — not just logical values).

### A/B parity harness

Add a test helper that takes a query and input rows and:
1. Evaluates with V1, captures output vector.
2. Evaluates with V2, captures output vector.
3. Asserts logical equality (`VectorEqualValueChecker`), null pattern
   equality, and where possible encoding equality.

Wire the existing `expression_test` to run under both flags in CI.

### Debug-build invariants

Compile CI debug builds with `DebugRemainingRowsGuard` and
`DebugEvaluateGuard` enabled.  These catch the most likely refactor
regressions (row-space violations, missing `releaseInputValues`,
result type mismatch) at every phase boundary.

### Production safety

The `expr_eval_v2` flag is per-query.  A canary deploy can route a
small percentage of production queries through V2 while V1 remains
the default; any divergence is logged via the existing query-fingerprint
infrastructure.

## Risks and open questions

### Risks

- **`ExprRuntimeState` threading.**  V1 today is single-threaded per
  `ExprSet` because state lives on `Expr`.  V2 enables concurrent
  evaluation, but `ExprSetV2` must decide: per-thread runtime-state
  trees, or one shared tree with a mutex.  The right answer depends on
  whether shared-subexpr / memo benefits cross threads.  Decision
  deferred to step 4 when the first concurrent caller appears.
- **Special-form delegation overhead.**  Each special-form evaluation
  in V2 calls `legacySpecialForm_->evalSpecialForm`, which does its
  own internal pipeline work.  Should be zero overhead in practice
  (one extra virtual call), but worth measuring at M5.
- **Stats merging.**  V1 accumulates stats on `Expr::stats_`.  V2
  accumulates on `ExprRuntimeState::stats`.  The reporting code
  (`ExprSet::stats()`) must be ported to read from the V2 location
  while V1 is still in tree.  Plan: read both, prefer V2 when
  populated.
- **Adapter cost on hot paths.**  `ExprV2::from(Expr)` walks the tree
  once per `ExprSetV2` construction.  Negligible relative to eval cost,
  but worth confirming on micro-benchmarks before M6.

### Open questions

- Should `ExprRuntimeStateTree` use flat-vector indexing (cheap, but
  requires stable tree iteration order) or `F14FastMap<ExprV2*>`
  (more flexible, slight lookup cost)?  Decide at step 2.
- Should `ExprV2` be `final`?  No subclasses are anticipated; making it
  `final` enables devirtualization in tight loops.
- Are there any callers that hold raw `Expr*` pointers across a query
  lifetime?  Audit needed before step 14.
- `FieldReference` is currently a subclass of `Expr` with custom
  `evalSpecialForm`.  Step 12 needs to decide whether `FieldReferenceV2`
  is a distinct `ExprV2` subtype, or whether `ExprV2` carries a
  `FieldReference` flag inline.

## Appendix: file layout

```
velox/expression/
  Expr.{h,cpp}                       — unchanged
  ExprSet.{h,cpp}                    — unchanged

  ExprV2.{h,cpp}                     — new: node + ExprV2::from(Expr)
  ExprRuntimeState.{h,cpp}           — new: per-node mutable state
  EvalFrame.{h,cpp}                  — new: per-call state
  ExprEvaluatorV2.{h,cpp}            — new: pipeline orchestrator
  ExprSetV2.{h,cpp}                  — new: owner type

  ExprPhases/                        — new: pipeline phases
    FastPath.{h,cpp}
    LazyInput.{h,cpp}
    FieldPeeling.{h,cpp}
    DictionaryMemo.{h,cpp}
    NullPruning.{h,cpp}
    SharedSubexprCache.{h,cpp}
    ArgEval.{h,cpp}
    ArgPeeling.{h,cpp}

  tests/
    ExprPipelineEdgeCasesTest.cpp    — new: step-1 golden tests
    ExprV2ParityTest.cpp             — new: A/B harness
```