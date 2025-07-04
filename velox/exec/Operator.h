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

#include <folly/Synchronized.h>
#include "velox/core/PlanNode.h"
#include "velox/exec/Driver.h"
#include "velox/exec/JoinBridge.h"
#include "velox/exec/OperatorStats.h"
#include "velox/exec/OperatorTraceWriter.h"
#include "velox/type/Filter.h"

namespace facebook::velox::exec {

/// Represents a column that is copied from input to output, possibly
/// with cardinality change, i.e. values removed or duplicated.
struct IdentityProjection {
  IdentityProjection(
      column_index_t _inputChannel,
      column_index_t _outputChannel)
      : inputChannel(_inputChannel), outputChannel(_outputChannel) {}

  column_index_t inputChannel;
  column_index_t outputChannel;
};

class OperatorCtx {
 public:
  OperatorCtx(
      DriverCtx* driverCtx,
      const core::PlanNodeId& planNodeId,
      int32_t operatorId,
      const std::string& operatorType = "");

  const std::shared_ptr<Task>& task() const {
    return driverCtx_->task;
  }

  const std::string& taskId() const;

  Driver* driver() const {
    return driverCtx_->driver;
  }

  DriverCtx* driverCtx() const {
    return driverCtx_;
  }

  velox::memory::MemoryPool* pool() const {
    return pool_;
  }

  const core::PlanNodeId& planNodeId() const {
    return planNodeId_;
  }

  int32_t operatorId() const {
    return operatorId_;
  }

  /// Sets operatorId. The use is limited to renumbering operators from
  /// DriverAdapter. Do not use outside of this.
  void setOperatorIdFromAdapter(int32_t id) {
    operatorId_ = id;
  }

  const std::string& operatorType() const {
    return operatorType_;
  }

  core::ExecCtx* execCtx() const;

  /// Makes an extract of QueryCtx for use in a connector. 'planNodeId'
  /// is the id of the calling TableScan. This and the task id identify the scan
  /// for column access tracking. 'connectorPool' is an aggregate memory pool
  /// for connector use.
  std::shared_ptr<connector::ConnectorQueryCtx> createConnectorQueryCtx(
      const std::string& connectorId,
      const std::string& planNodeId,
      memory::MemoryPool* connectorPool,
      const common::SpillConfig* spillConfig = nullptr) const;

 private:
  DriverCtx* const driverCtx_;
  const core::PlanNodeId planNodeId_;
  int32_t operatorId_;
  const std::string operatorType_;
  velox::memory::MemoryPool* const pool_;

  // These members are created on demand.
  mutable std::unique_ptr<core::ExecCtx> execCtx_;
};

/// Query operator
class Operator : public BaseRuntimeStatWriter {
 public:
  /// Factory class for mapping a user-registered PlanNode into the
  /// corresponding Operator.
  class PlanNodeTranslator {
   public:
    virtual ~PlanNodeTranslator() = default;

    /// Translates plan node to operator. Returns nullptr if the plan node
    /// cannot be handled by this factory.
    virtual std::unique_ptr<Operator>
    toOperator(DriverCtx* ctx, int32_t id, const core::PlanNodePtr& node) {
      return nullptr;
    }

    /// An overloaded method that should be called when the operator needs an
    /// ExchangeClient.
    virtual std::unique_ptr<Operator> toOperator(
        DriverCtx* ctx,
        int32_t id,
        const core::PlanNodePtr& node,
        std::shared_ptr<ExchangeClient> exchangeClient) {
      return nullptr;
    }

    /// Translates plan node to join bridge. Returns nullptr if the plan node
    /// cannot be handled by this factory.
    virtual std::unique_ptr<JoinBridge> toJoinBridge(
        const core::PlanNodePtr& /* node */) {
      return nullptr;
    }

    /// Translates plan node to operator supplier. Returns nullptr if the plan
    /// node cannot be handled by this factory.
    virtual OperatorSupplier toOperatorSupplier(
        const core::PlanNodePtr& /* node */) {
      return nullptr;
    }

    /// Returns max driver count for the plan node. Returns std::nullopt if the
    /// plan node cannot be handled by this factory.
    virtual std::optional<uint32_t> maxDrivers(
        const core::PlanNodePtr& /* node */) {
      return std::nullopt;
    }
  };

  /// The name of the runtime spill stats collected and reported by operators
  /// that support spilling.

  /// This indicates the spill not supported for a spillable operator when the
  /// spill config is enabled. This is due to the spill limitation in certain
  /// plan node config such as unpartition window operator.
  static inline const std::string kSpillNotSupported{"spillNotSupported"};
  /// The spill write stats.
  static inline const std::string kSpillFillTime{"spillFillWallNanos"};
  static inline const std::string kSpillSortTime{"spillSortWallNanos"};
  static inline const std::string kSpillExtractVectorTime{
      "spillExtractVectorWallNanos"};
  static inline const std::string kSpillSerializationTime{
      "spillSerializationWallNanos"};
  static inline const std::string kSpillFlushTime{"spillFlushWallNanos"};
  static inline const std::string kSpillWrites{"spillWrites"};
  static inline const std::string kSpillWriteTime{"spillWriteWallNanos"};
  static inline const std::string kSpillRuns{"spillRuns"};
  static inline const std::string kExceededMaxSpillLevel{
      "exceededMaxSpillLevel"};
  /// The spill read stats.
  static inline const std::string kSpillReadBytes{"spillReadBytes"};
  static inline const std::string kSpillReads{"spillReads"};
  static inline const std::string kSpillReadTime{"spillReadWallNanos"};
  static inline const std::string kSpillDeserializationTime{
      "spillDeserializationWallNanos"};

  /// The vector serde kind used by an operator for shuffle. The recorded
  /// runtime stats value is the corresponding enum value.
  static inline const std::string kShuffleSerdeKind{"shuffleSerdeKind"};

  /// The compression kind used by an operator for shuffle. The recorded
  /// runtime stats value is the corresponding enum value.
  static inline const std::string kShuffleCompressionKind{
      "shuffleCompressionKind"};

  /// 'operatorId' is the initial index of the 'this' in the Driver's list of
  /// Operators. This is used as in index into OperatorStats arrays in the Task.
  /// 'planNodeId' is a query-level unique identifier of the PlanNode to which
  /// 'this' corresponds. 'operatorType' is a label for use in stats. If
  /// 'canSpill' is true, then disk spilling is allowed for this operator.
  ///
  /// NOTE: the operator (and any derived operator class) constructor should
  /// not allocate memory from memory pool. The latter might trigger memory
  /// arbitration operation that can lead to deadlock as both operator
  /// construction and operator memory reclaim need to acquire task lock.
  Operator(
      DriverCtx* driverCtx,
      RowTypePtr outputType,
      int32_t operatorId,
      std::string planNodeId,
      std::string operatorType,
      std::optional<common::SpillConfig> spillConfig = std::nullopt);

  virtual ~Operator() = default;

  /// Does initialization work for this operator which requires memory
  /// allocation from memory pool that can't be done under operator constructor.
  ///
  /// NOTE: the default implementation set 'initialized_' to true to ensure we
  /// never call this more than once. The overload initialize() implementation
  /// must call this base implementation first.
  virtual void initialize();

  /// Indicates if this operator has been initialized or not.
  bool isInitialized() const {
    return initialized_;
  }

  /// Returns true if 'this' can accept input. Not used if operator is a source
  /// operator, e.g. the first operator in the pipeline.
  virtual bool needsInput() const = 0;

  /// Adds input. Not used if operator is a source operator, e.g. the first
  /// operator in the pipeline.
  /// @param input Non-empty input vector.
  virtual void addInput(RowVectorPtr input) = 0;

  /// Informs 'this' that addInput will no longer be called. This means
  /// that any partial state kept by 'this' should be returned by
  /// the next call(s) to getOutput. Not used if operator is a source operator,
  /// e.g. the first operator in the pipeline.
  virtual void noMoreInput() {
    noMoreInput_ = true;
  }

  /// Invoked by the driver to start draining output on this operator. The
  /// function returns true if this operator has buffered output to drain.
  /// Otherwise false and the driver directly proceeds to the next operator to
  /// drain.
  virtual bool startDrain() {
    VELOX_NYI();
    return false;
  }

  /// Returns true if this operator is draining output.
  bool isDraining() const;

  /// Returns true if this operator has drained all its buffered output, and the
  /// associated driver is still draining.
  bool hasDrained() const;

  /// Returns true if this operator needs to drop output. This is used to
  /// drop all the input processing for a draining operator which doesn't
  /// need any more input to quickly finish the drain operation, e.g. a draining
  /// merge join operator decides to drop inputs from both sides when it
  /// can't produce any more match outputs.
  bool shouldDropOutput() const;

  /// Returns a RowVector with the result columns. Returns nullptr if
  /// no more output can be produced without more input or if blocked
  /// for outside causes. isBlocked distinguishes between the
  /// cases. Sink operator, e.g. the last operator in the pipeline, must return
  /// nullptr and pass results to the consumer through a custom mechanism.
  /// @return nullptr or a non-empty output vector.
  virtual RowVectorPtr getOutput() = 0;

  /// Returns kNotBlocked if 'this' is not prevented from
  /// advancing. Otherwise, returns a reason and sets 'future' to a
  /// future that will be realized when the reason is no longer present.
  /// The caller must wait for the `future` to complete before making
  /// another call.
  virtual BlockingReason isBlocked(ContinueFuture* future) = 0;

  /// Returns true if completely finished processing and no more output will be
  /// produced. Some operators may finish early before receiving all input and
  /// noMoreInput() message. For example, Limit operator finishes as soon as it
  /// receives specified number of rows and HashProbe finishes early if the
  /// build side is empty.
  virtual bool isFinished() = 0;

  /// True if the operator is in dry run mode which is only used by input
  /// trace collection for crash debugging.
  bool dryRun() const {
    return dryRun_;
  }

  /// Traces input batch of the operator.
  virtual void traceInput(const RowVectorPtr&);

  /// Finishes tracing of the operator.
  virtual void finishTrace();

  /// Returns true if this operator would accept a filter dynamically generated
  /// by a downstream operator.
  virtual bool canAddDynamicFilter() const {
    return false;
  }

  /// Adds pending filters dynamically generated by a downstream
  /// operator. Called only if canAddDynamicFilter() returns true. Shared lock
  /// on the PushdownFilters is already held by current thread when this method
  /// is called by driver.
  virtual void addDynamicFilterLocked(
      const core::PlanNodeId& /*producer*/,
      const PushdownFilters& /*filters*/) {
    VELOX_UNSUPPORTED(
        "This operator doesn't support dynamic filter pushdown: {}",
        toString());
  }

  /// Returns a list of identity projections, e.g. columns that are projected
  /// as-is possibly after applying a filter. Used to allow pushdown of dynamic
  /// filters generated by HashProbe into the TableScan. Examples of identity
  /// projections: all columns in FilterProject(only filters), group-by keys in
  /// HashAggregation.
  const std::vector<IdentityProjection>& identityProjections() const {
    return identityProjections_;
  }

  /// Frees all resources associated with 'this'. No other methods
  /// should be called after this.
  virtual void close();

  /// Returns true if 'this' never has more output rows than input rows.
  virtual bool isFilter() const {
    return false;
  }

  virtual bool preservesOrder() const {
    return false;
  }

  /// Returns copy of operator stats. If 'clear' is true, the function also
  /// clears the operator stats after retrieval.
  virtual OperatorStats stats(bool clear);

  /// Add a single runtime stat to the operator stats under the write lock.
  /// This member overrides BaseRuntimeStatWriter's member.
  void addRuntimeStat(const std::string& name, const RuntimeCounter& value)
      override {
    stats_.wlock()->addRuntimeStat(name, value);
  }

  /// Returns reference to the operator stats synchronized object to gain bulk
  /// read/write access to the stats.
  folly::Synchronized<OperatorStats>& stats() {
    return stats_;
  }

  void recordBlockingTime(uint64_t start, BlockingReason reason);

  virtual std::string toString() const;

  /// Used in debug endpoints.
  virtual folly::dynamic toJson() const {
    folly::dynamic obj = folly::dynamic::object;
    obj["operator"] = toString();
    return obj;
  }

  velox::memory::MemoryPool* pool() const {
    return operatorCtx_->pool();
  }

  /// Returns true if the operator is reclaimable. Currently, we only support
  /// to reclaim memory from a spillable operator.
  FOLLY_ALWAYS_INLINE virtual bool canReclaim() const {
    return canSpill();
  }

  /// Returns how many bytes is reclaimable from this operator. The function
  /// returns true if this operator is reclaimable, and returns the estimated
  /// reclaimable bytes.
  virtual bool reclaimableBytes(uint64_t& reclaimableBytes) const {
    const bool reclaimable = canReclaim();
    reclaimableBytes = reclaimable ? pool()->reservedBytes() : 0;
    return reclaimable;
  }

  /// Invoked by the memory arbitrator to reclaim memory from this operator with
  /// specified reclaim target bytes. If 'targetBytes' is zero, then it tries to
  /// reclaim all the reclaimable memory from this operator.
  ///
  /// NOTE: this method doesn't return the actually freed memory bytes. The
  /// caller need to claim the actually freed memory space by shrinking the
  /// associated root memory pool's capacity accordingly.
  virtual void reclaim(
      uint64_t targetBytes,
      memory::MemoryReclaimer::Stats& stats) {}

  const core::PlanNodeId& planNodeId() const {
    return operatorCtx_->planNodeId();
  }

  int32_t operatorId() const {
    return operatorCtx_->operatorId();
  }

  uint32_t splitGroupId() const {
    return operatorCtx_->driverCtx()->splitGroupId;
  }

  /// Sets operator id. Use is limited to renumbering Operators from
  /// DriverAdapter. Do not use outside of this.
  void setOperatorIdFromAdapter(int32_t id) {
    operatorCtx_->setOperatorIdFromAdapter(id);
    stats().wlock()->operatorId = id;
  }

  const std::string& operatorType() const {
    return operatorCtx_->operatorType();
  }

  const std::string& taskId() const {
    return operatorCtx_->taskId();
  }

  /// Registers 'translator' for mapping user defined PlanNode subclass
  /// instances to user-defined Operators.
  static void registerOperator(std::unique_ptr<PlanNodeTranslator> translator);

  /// Removes all translators registered earlier via calls to
  /// 'registerOperator'.
  static void unregisterAllOperators();

  /// Calls all the registered PlanNodeTranslators on 'planNode' and returns the
  /// result of the first one that returns non-nullptr or nullptr if all return
  /// nullptr. exchangeClient is not-null only when
  /// planNode->requiresExchangeClient() is true.
  static std::unique_ptr<Operator> fromPlanNode(
      DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& planNode,
      std::shared_ptr<ExchangeClient> exchangeClient = nullptr);

  /// Calls all the registered PlanNodeTranslators on 'planNode' and returns the
  /// result of the first one that returns non-nullptr or nullptr if all return
  /// nullptr.
  static std::unique_ptr<JoinBridge> joinBridgeFromPlanNode(
      const core::PlanNodePtr& planNode);

  /// Calls all the registered PlanNodeTranslators on 'planNode' and returns the
  /// result of the first one that returns non-nullptr or nullptr if all return
  /// nullptr.
  static OperatorSupplier operatorSupplierFromPlanNode(
      const core::PlanNodePtr& planNode);

  /// Calls `maxDrivers` on all the registered PlanNodeTranslators and returns
  /// the first one that is not std::nullopt or std::nullopt otherwise.
  static std::optional<uint32_t> maxDrivers(const core::PlanNodePtr& planNode);

  /// The scoped objects to mark an operator is under non-reclaimable execution
  /// section or not. This prevents the memory arbitrator from reclaiming memory
  /// from the operator if it happens to be suspended for memory arbitration
  /// processing. The driver execution framework marks an operator under
  /// non-reclaimable section when executes any of its method. The spillable
  /// operator might clear this temporarily during its execution to reserve
  /// memory from arbitrator to allow memory reclaim from itself.
  class ReclaimableSectionGuard {
   public:
    /// If 'enter' is true, marks 'op' is under non-reclaimable execution,
    /// otherwise not.
    ReclaimableSectionGuard(Operator* op)
        : op_(op), nonReclaimableSection_(op_->nonReclaimableSection_) {
      op_->nonReclaimableSection_ = false;
    }

    ~ReclaimableSectionGuard() {
      op_->nonReclaimableSection_ = nonReclaimableSection_;
    }

   private:
    Operator* const op_;
    const bool nonReclaimableSection_;
  };

  class NonReclaimableSectionGuard {
   public:
    NonReclaimableSectionGuard(Operator* op)
        : op_(op), nonReclaimableSection_(op_->nonReclaimableSection_) {
      op_->nonReclaimableSection_ = true;
    }

    ~NonReclaimableSectionGuard() {
      op_->nonReclaimableSection_ = nonReclaimableSection_;
    }

   private:
    Operator* const op_;
    const bool nonReclaimableSection_;
  };

  /// Returns the operator context of this operator.
  const OperatorCtx* operatorCtx() const {
    return operatorCtx_.get();
  }

  /// Returns true if this operator has received no more input signal. This
  /// method is only used for test.
  bool testingNoMoreInput() const {
    return noMoreInput_;
  }

  /// Returns true if this operator is under non-reclaimable section, otherwise
  /// not. This method is only used for test.
  bool testingNonReclaimable() const {
    return nonReclaimableSection_;
  }

  bool testingHasInput() const {
    return input_ != nullptr;
  }

 protected:
  static std::vector<std::unique_ptr<PlanNodeTranslator>>& translators();
  friend class NonReclaimableSection;

  class MemoryReclaimer : public memory::MemoryReclaimer {
   public:
    static std::unique_ptr<memory::MemoryReclaimer> create(
        DriverCtx* driverCtx,
        Operator* op);

    void enterArbitration() override;

    void leaveArbitration() noexcept override;

    bool reclaimableBytes(
        const memory::MemoryPool& pool,
        uint64_t& reclaimableBytes) const override;

    uint64_t reclaim(
        memory::MemoryPool* pool,
        uint64_t targetBytes,
        uint64_t maxWaitMs,
        memory::MemoryReclaimer::Stats& stats) override;

    void abort(memory::MemoryPool* pool, const std::exception_ptr& /* error */)
        override;

   protected:
    MemoryReclaimer(const std::shared_ptr<Driver>& driver, Operator* op)
        : memory::MemoryReclaimer(0), driver_(driver), op_(op) {
      VELOX_CHECK_NOT_NULL(op_);
    }

    // Gets the shared pointer to the associated driver to ensure the liveness
    // of the operator during the memory reclaim operation.
    //
    // NOTE: an operator's memory pool can outlive its operator.
    std::shared_ptr<Driver> ensureDriver() const {
      return driver_.lock();
    }

    const std::weak_ptr<Driver> driver_;
    Operator* const op_;
  };

  /// Invoked to setup memory reclaimer for this operator's memory pool if its
  /// parent node memory pool has set the reclaimer.
  void maybeSetReclaimer();

  /// Returns true if this is a spillable operator and has configured spilling.
  FOLLY_ALWAYS_INLINE virtual bool canSpill() const {
    return spillConfig_.has_value();
  }

  const common::SpillConfig* spillConfig() const {
    return spillConfig_.has_value() ? &spillConfig_.value() : nullptr;
  }

  /// Invoked to setup query data or split writer for this operator if the
  /// associated query plan node is configured to collect trace.
  void maybeSetTracer();

  /// Creates output vector from 'input_' and 'results' according to
  /// 'identityProjections_' and 'resultProjections_'. If 'mapping' is set to
  /// nullptr, the children of the output vector will be identical to their
  /// respective sources from 'input_' or 'results'. However, if 'mapping' is
  /// provided, the children of the output vector will be generated as
  /// dictionary of the sources using the specified 'mapping'.
  RowVectorPtr fillOutput(
      vector_size_t size,
      const BufferPtr& mapping,
      const std::vector<VectorPtr>& results);

  /// Creates output vector from 'input_' and 'results_' according to
  /// 'identityProjections_' and 'resultProjections_'.
  RowVectorPtr fillOutput(vector_size_t size, const BufferPtr& mapping);

  /// Invoked by the operator to notify driver that it has finished draining
  /// output.
  virtual void finishDrain();

  /// Returns the number of rows for the output batch. This uses averageRowSize
  /// to calculate how many rows fit in preferredOutputBatchBytes. It caps the
  /// number of rows at 10K and returns at least one row. The averageRowSize
  /// must not be negative. If the averageRowSize is 0 which is not advised,
  /// returns maxOutputBatchRows. If the averageRowSize is not given, returns
  /// preferredOutputBatchRows.
  vector_size_t outputBatchRows(
      std::optional<uint64_t> averageRowSize = std::nullopt) const;

  /// Load 'vector' if lazy. Potential large memory usage site that might
  /// trigger arbitration. Hence this call is made reclaimable. This is often
  /// used at the start of 'addInput()' call to load the input if needed.
  ///
  /// NOTE: Caller must make sure operator is in a reclaimable state when making
  /// this call.
  void loadLazyReclaimable(RowVectorPtr& vector);

  /// Invoked to record spill stats in operator stats.
  virtual void recordSpillStats();

  const std::unique_ptr<OperatorCtx> operatorCtx_;
  const RowTypePtr outputType_;
  /// Contains the disk spilling related configs if spilling is enabled (e.g.
  /// the fs dir path to store spill files), otherwise null.
  const std::optional<common::SpillConfig> spillConfig_;

  const bool dryRun_;

  bool initialized_{false};

  folly::Synchronized<OperatorStats> stats_;
  std::shared_ptr<folly::Synchronized<common::SpillStats>> spillStats_ =
      std::make_shared<folly::Synchronized<common::SpillStats>>();

  /// NOTE: only one of the two could be set for an operator for tracing .
  /// 'splitTracer_' is only set for table scan to record the processed split
  /// for now.
  std::unique_ptr<trace::OperatorTraceInputWriter> inputTracer_{nullptr};
  std::unique_ptr<trace::OperatorTraceSplitWriter> splitTracer_{nullptr};

  /// Indicates if an operator is under a non-reclaimable execution section.
  /// This prevents the memory arbitrator from reclaiming memory from this
  /// operator if it happens to be suspended for memory arbitration processing.
  /// This only applies to a reclaimable operator.
  tsan_atomic<bool> nonReclaimableSection_{false};

  /// Holds the last data from addInput until it is processed. Reset after the
  /// input is processed.
  RowVectorPtr input_;

  bool noMoreInput_ = false;
  std::vector<IdentityProjection> identityProjections_;
  std::vector<VectorPtr> results_;

  /// Maps between index in results_ and index in output RowVector.
  std::vector<IdentityProjection> resultProjections_;

  /// True if the input and output rows have exactly the same fields, i.e. one
  /// could copy directly from input to output if no cardinality change.
  bool isIdentityProjection_ = false;

 private:
  // Setup 'inputTracer_' to record the processed input vectors.
  void setupInputTracer(const std::string& traceDir);
  // Setup 'splitTracer_' for table scan to record the processed split.
  void setupSplitTracer(const std::string& traceDir);
};

/// Given a row type returns indices for the specified subset of columns.
std::vector<column_index_t> toChannels(
    const RowTypePtr& rowType,
    const std::vector<core::TypedExprPtr>& exprs);

column_index_t exprToChannel(const core::ITypedExpr* expr, const TypePtr& type);

/// Given a source output type and target input type we return the indices of
/// the target input columns in the source output type.
/// The target output type is used to determine if the projection is identity.
/// An empty indices vector is returned when projection is identity.
std::vector<column_index_t> calculateOutputChannels(
    const RowTypePtr& sourceOutputType,
    const RowTypePtr& targetInputType,
    const RowTypePtr& targetOutputType);

/// A first operator in a Driver, e.g. table scan or exchange client.
class SourceOperator : public Operator {
 public:
  SourceOperator(
      DriverCtx* driverCtx,
      RowTypePtr outputType,
      int32_t operatorId,
      const std::string& planNodeId,
      const std::string& operatorType,
      const std::optional<common::SpillConfig>& spillConfig = std::nullopt)
      : Operator(
            driverCtx,
            std::move(outputType),
            operatorId,
            planNodeId,
            operatorType,
            spillConfig) {}

  bool needsInput() const override {
    return false;
  }

  void addInput(RowVectorPtr /* unused */) override {
    VELOX_FAIL("SourceOperator does not support addInput()");
  }

  void noMoreInput() override {
    VELOX_FAIL("SourceOperator does not support noMoreInput()");
  }
};
} // namespace facebook::velox::exec

template <>
struct fmt::formatter<std::thread::id> : formatter<std::string> {
  auto format(std::thread::id s, format_context& ctx) const {
    std::ostringstream oss;
    oss << s;
    return formatter<std::string>::format(oss.str(), ctx);
  }
};
