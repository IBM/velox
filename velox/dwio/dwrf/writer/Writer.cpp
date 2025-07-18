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

#include "velox/dwio/dwrf/writer/Writer.h"

#include <folly/ScopeGuard.h>

#include "velox/common/base/Counters.h"
#include "velox/common/base/Pointers.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/common/time/CpuWallTimer.h"
#include "velox/dwio/dwrf/common/Common.h"
#include "velox/dwio/dwrf/utils/ProtoUtils.h"
#include "velox/dwio/dwrf/writer/FlushPolicy.h"
#include "velox/dwio/dwrf/writer/LayoutPlanner.h"
#include "velox/exec/MemoryReclaimer.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::dwrf {

namespace {

dwio::common::StripeProgress getStripeProgress(const WriterContext& context) {
  return dwio::common::StripeProgress{
      .stripeIndex = context.stripeIndex(),
      .stripeRowCount = context.stripeRowCount(),
      .totalMemoryUsage = context.getTotalMemoryUsage(),
      .stripeSizeEstimate = context.linearStripeSizeHeuristics()
          ? std::max(
                context.getEstimatedStripeSize(context.stripeRawSize()),
                // The stripe size estimate is only more accurate from the
                // second stripe onward because it uses past stripe states in
                // heuristics. We need to additionally bound it with output
                // stream size based estimate for the first stripe.
                context.stripeIndex() == 0
                    ? context.getEstimatedOutputStreamSize()
                    : 0)
          : context.getEstimatedOutputStreamSize()};
}

uint64_t orcWriterMaxStripeSize(
    const config::ConfigBase& config,
    const config::ConfigBase& session) {
  return config::toCapacity(
      session.get<std::string>(
          dwrf::Config::kOrcWriterMaxStripeSizeSession,
          config.get<std::string>(
              dwrf::Config::kOrcWriterMaxStripeSize, "64MB")),
      config::CapacityUnit::BYTE);
}

uint64_t orcWriterMaxDictionaryMemory(
    const config::ConfigBase& config,
    const config::ConfigBase& session) {
  return config::toCapacity(
      session.get<std::string>(
          dwrf::Config::kOrcWriterMaxDictionaryMemorySession,
          config.get<std::string>(
              dwrf::Config::kOrcWriterMaxDictionaryMemory, "16MB")),
      config::CapacityUnit::BYTE);
}

bool isOrcWriterIntegerDictionaryEncodingEnabled(
    const config::ConfigBase& config,
    const config::ConfigBase& session) {
  return session.get<bool>(
      dwrf::Config::kOrcWriterIntegerDictionaryEncodingEnabledSession,
      config.get<bool>(
          dwrf::Config::kOrcWriterIntegerDictionaryEncodingEnabled, true));
}

bool isOrcWriterStringDictionaryEncodingEnabled(
    const config::ConfigBase& config,
    const config::ConfigBase& session) {
  return session.get<bool>(
      dwrf::Config::kOrcWriterStringDictionaryEncodingEnabledSession,
      config.get<bool>(
          dwrf::Config::kOrcWriterStringDictionaryEncodingEnabled, true));
}

bool orcWriterLinearStripeSizeHeuristics(
    const config::ConfigBase& config,
    const config::ConfigBase& session) {
  return session.get<bool>(
      dwrf::Config::kOrcWriterLinearStripeSizeHeuristicsSession,
      config.get<bool>(
          dwrf::Config::kOrcWriterLinearStripeSizeHeuristics, true));
}

uint64_t orcWriterMinCompressionSize(
    const config::ConfigBase& config,
    const config::ConfigBase& session) {
  return session.get<uint64_t>(
      dwrf::Config::kOrcWriterMinCompressionSizeSession,
      config.get<uint64_t>(dwrf::Config::kOrcWriterMinCompressionSize, 1024));
}

std::optional<uint8_t> orcWriterCompressionLevel(
    const config::ConfigBase& config,
    const config::ConfigBase& session) {
  auto sessionProp =
      session.get<uint8_t>(dwrf::Config::kOrcWriterCompressionLevelSession);

  if (sessionProp.has_value()) {
    return sessionProp.value();
  }

  auto configProp =
      config.get<uint8_t>(dwrf::Config::kOrcWriterCompressionLevel);

  if (configProp.has_value()) {
    return configProp.value();
  }

  // Presto has a single config controlling this value, but different defaults
  // depending on the compression kind.
  return std::nullopt;
}

uint8_t orcWriterZLIBCompressionLevel(
    const config::ConfigBase& config,
    const config::ConfigBase& session) {
  constexpr uint8_t kDefaultZlibCompressionLevel = 4;
  return orcWriterCompressionLevel(config, session)
      .value_or(kDefaultZlibCompressionLevel);
}

uint8_t orcWriterZSTDCompressionLevel(
    const config::ConfigBase& config,
    const config::ConfigBase& session) {
  constexpr uint8_t kDefaultZstdCompressionLevel = 3;
  return orcWriterCompressionLevel(config, session)
      .value_or(kDefaultZstdCompressionLevel);
}

#define NON_RECLAIMABLE_SECTION_CHECK() \
  VELOX_CHECK(nonReclaimableSection_ == nullptr || *nonReclaimableSection_);
} // namespace

Writer::Writer(
    std::unique_ptr<dwio::common::FileSink> sink,
    const WriterOptions& options,
    std::shared_ptr<memory::MemoryPool> pool)
    : writerBase_(std::make_unique<WriterBase>(std::move(sink))),
      schema_{dwio::common::TypeWithId::create(options.schema)},
      spillConfig_{options.spillConfig},
      nonReclaimableSection_(options.nonReclaimableSection) {
  VELOX_CHECK(
      spillConfig_ == nullptr || nonReclaimableSection_ != nullptr,
      "nonReclaimableSection_ must be set if writer memory reclaim is enabled");
  auto handler =
      (options.encryptionSpec ? encryption::EncryptionHandler::create(
                                    schema_,
                                    *options.encryptionSpec,
                                    options.encrypterFactory.get())
                              : nullptr);
  writerBase_->initContext(
      options.config,
      pool,
      options.sessionTimezone,
      options.adjustTimestampToTimezone,
      std::move(handler));
  auto& context = writerBase_->getContext();
  VELOX_CHECK_EQ(
      context.getTotalMemoryUsage(),
      0,
      "Unexpected memory usage on dwrf writer construction");
  setMemoryReclaimers(pool);
  writerBase_->initBuffers();

  context.buildPhysicalSizeAggregators(*schema_);
  if (options.flushPolicyFactory == nullptr) {
    flushPolicy_ = std::make_unique<DefaultFlushPolicy>(
        context.stripeSizeFlushThreshold(),
        context.dictionarySizeFlushThreshold());
  } else {
    castUniquePointer(options.flushPolicyFactory(), flushPolicy_);
  }

  if (options.layoutPlannerFactory != nullptr) {
    layoutPlanner_ = options.layoutPlannerFactory(*schema_);
  } else {
    layoutPlanner_ = std::make_unique<LayoutPlanner>(*schema_);
  }

  if (options.columnWriterFactory == nullptr) {
    writer_ = BaseColumnWriter::create(
        writerBase_->getContext(),
        *schema_,
        /*sequence=*/0,
        /*onRecordPosition=*/nullptr,
        options.format);
  } else {
    writer_ = options.columnWriterFactory(writerBase_->getContext(), *schema_);
  }
  setState(State::kRunning);
}

Writer::Writer(
    std::unique_ptr<dwio::common::FileSink> sink,
    const WriterOptions& options)
    : Writer{
          std::move(sink),
          options,
          options.memoryPool->addAggregateChild(fmt::format(
              "{}.dwrf.{}",
              options.memoryPool->name(),
              folly::to<std::string>(folly::Random::rand64())))} {}

void Writer::setMemoryReclaimers(
    const std::shared_ptr<memory::MemoryPool>& pool) {
  VELOX_CHECK(
      !pool->isLeaf(),
      "The root memory pool for dwrf writer can't be leaf: {}",
      pool->name());
  VELOX_CHECK_NULL(pool->reclaimer());

  if ((pool->parent() == nullptr) || (pool->parent()->reclaimer() == nullptr)) {
    return;
  }

  pool->setReclaimer(MemoryReclaimer::create(this));
  auto& context = getContext();
  context.getMemoryPool(MemoryUsageCategory::GENERAL)
      .setReclaimer(exec::MemoryReclaimer::create());
  context.getMemoryPool(MemoryUsageCategory::DICTIONARY)
      .setReclaimer(exec::MemoryReclaimer::create());
  context.getMemoryPool(MemoryUsageCategory::OUTPUT_STREAM)
      .setReclaimer(exec::MemoryReclaimer::create());
}

void Writer::write(const VectorPtr& input) {
  checkRunning();
  NON_RECLAIMABLE_SECTION_CHECK();

  auto& context = writerBase_->getContext();
  // Calculate length increment based on linear projection of micro batch size.
  // Total length is capped later.
  const auto& estimatedInputMemoryBytes = input->estimateFlatSize();
  const auto inputRowCount = input->size();
  const size_t writeBatchSize = std::max<size_t>(
      1UL,
      estimatedInputMemoryBytes > 0
          ? folly::to<size_t>(std::floor(
                1.0 * context.rawDataSizePerBatch() /
                estimatedInputMemoryBytes * inputRowCount))
          : folly::to<size_t>(inputRowCount));
  if (FOLLY_UNLIKELY(
          estimatedInputMemoryBytes == 0 ||
          estimatedInputMemoryBytes > context.rawDataSizePerBatch())) {
    VLOG(1) << fmt::format(
        "Unpopulated or huge vector memory estimate! Micro write batch size {} rows. "
        "Input vector memory estimate {} bytes. Batching threshold {} bytes.",
        writeBatchSize,
        estimatedInputMemoryBytes,
        context.rawDataSizePerBatch());
  }

  size_t rowOffset = 0;
  while (rowOffset < inputRowCount) {
    size_t numRowsToWrite = writeBatchSize;
    if (context.indexEnabled()) {
      // Do not write cross an index row block.
      numRowsToWrite = std::min<size_t>(
          numRowsToWrite, context.indexStride() - context.indexRowCount());
    }

    numRowsToWrite = std::min(numRowsToWrite, inputRowCount - rowOffset);
    VELOX_CHECK_GT(numRowsToWrite, 0);

    ensureWriteFits(
        estimatedInputMemoryBytes * numRowsToWrite / inputRowCount,
        numRowsToWrite);

    TestValue::adjust("facebook::velox::dwrf::Writer::write", this);

    bool doFlush = shouldFlush(context, numRowsToWrite);
    if (doFlush) {
      // TODO: this is likely not needed after the early dictionary tests.
      // Should make the decision based on arbitration stats. Then we can
      // potential simplify a lot of logic around trimming. Try abandoning
      // inefficiency dictionary encodings early and see if we can delay the
      // flush.
      if (writer_->tryAbandonDictionaries(/*force=*/false)) {
        doFlush = shouldFlush(context, numRowsToWrite);
      }
      if (doFlush) {
        flush();
      }
    }

    const auto rawSize = writer_->write(
        input, common::Ranges::of(rowOffset, rowOffset + numRowsToWrite));
    rowOffset += numRowsToWrite;
    context.incRawSize(rawSize);

    if (context.indexEnabled() &&
        context.indexRowCount() >= context.indexStride()) {
      createRowIndexEntry();
    }
  }
}

bool Writer::canReclaim() const {
  return spillConfig_ != nullptr;
}

void Writer::ensureWriteFits(size_t appendBytes, size_t appendRows) {
  if (!canReclaim()) {
    return;
  }

  auto& context = getContext();
  const uint64_t totalMemoryUsage = context.getTotalMemoryUsage();
  if (totalMemoryUsage == 0) {
    return;
  }

  // Allows the memory arbitrator to reclaim memory from this file writer if the
  // memory reservation below has triggered memory arbitration.
  memory::ReclaimableSectionGuard reclaimGuard(nonReclaimableSection_);

  const size_t estimatedAppendMemoryBytes =
      std::max(appendBytes, context.estimateNextWriteSize(appendRows));
  const double estimatedMemoryGrowthRatio =
      static_cast<double>(estimatedAppendMemoryBytes) / totalMemoryUsage;
  if (!maybeReserveMemory(
          MemoryUsageCategory::GENERAL, estimatedMemoryGrowthRatio)) {
    return;
  }
  if (!maybeReserveMemory(
          MemoryUsageCategory::DICTIONARY, estimatedMemoryGrowthRatio)) {
    return;
  }
  if (!maybeReserveMemory(
          MemoryUsageCategory::OUTPUT_STREAM, estimatedMemoryGrowthRatio)) {
    return;
  }
}

void Writer::ensureStripeFlushFits() {
  if (!canReclaim()) {
    return;
  }
  if (memory::underMemoryArbitration()) {
    // NOTE: we skip memory reservation if the stripe flush is triggered by
    // memory arbitration.
    return;
  }

  // Allows the memory arbitrator to reclaim memory from this file writer if the
  // memory reservation below has triggered memory arbitration.
  memory::ReclaimableSectionGuard reclaimGuard(nonReclaimableSection_);

  auto& context = getContext();
  const size_t estimateFlushMemoryBytes =
      context.getEstimatedFlushOverhead(context.stripeRawSize());
  const uint64_t outputMemoryUsage =
      context.getMemoryUsage(MemoryUsageCategory::OUTPUT_STREAM);
  if (outputMemoryUsage == 0) {
    const uint64_t outputMemoryToReserve = estimateFlushMemoryBytes +
        estimateFlushMemoryBytes * spillConfig_->spillableReservationGrowthPct /
            100;
    context.getMemoryPool(MemoryUsageCategory::OUTPUT_STREAM)
        .maybeReserve(outputMemoryToReserve);
  } else {
    const double estimatedMemoryGrowthRatio =
        static_cast<double>(estimateFlushMemoryBytes) / outputMemoryUsage;
    maybeReserveMemory(
        MemoryUsageCategory::OUTPUT_STREAM, estimatedMemoryGrowthRatio);
  }
}

bool Writer::maybeReserveMemory(
    MemoryUsageCategory memoryUsageCategory,
    double estimatedMemoryGrowthRatio) {
  VELOX_CHECK(!*nonReclaimableSection_);
  VELOX_CHECK(canReclaim());
  auto& context = getContext();
  auto& pool = context.getMemoryPool(memoryUsageCategory);
  const uint64_t availableReservation = pool.availableReservation();
  const uint64_t usedBytes = pool.usedBytes();
  const uint64_t minReservationBytes =
      usedBytes * spillConfig_->minSpillableReservationPct / 100;
  const uint64_t estimatedIncrementBytes =
      usedBytes * estimatedMemoryGrowthRatio;
  if ((availableReservation > minReservationBytes) &&
      (availableReservation > 2 * estimatedIncrementBytes)) {
    return true;
  }

  const uint64_t bytesToReserve = std::max(
      estimatedIncrementBytes * 2,
      usedBytes * spillConfig_->spillableReservationGrowthPct / 100);
  return pool.maybeReserve(bytesToReserve);
}

int64_t Writer::releaseMemory() {
  if (!canReclaim()) {
    return 0;
  }
  return getContext().releaseMemoryReservation();
}

uint64_t Writer::flushTimeMemoryUsageEstimate(
    const WriterContext& context,
    size_t nextWriteSize) const {
  return context.getTotalMemoryUsage() +
      context.getEstimatedStripeSize(nextWriteSize) +
      context.getEstimatedFlushOverhead(
          context.stripeRawSize() + nextWriteSize);
}

bool Writer::overMemoryBudget(const WriterContext& context, size_t numRows)
    const {
  // Flush if we cannot take one additional slice/stride based on current stripe
  // raw size.
  const size_t nextWriteSize = context.estimateNextWriteSize(numRows);
  return flushTimeMemoryUsageEstimate(context, nextWriteSize) >
      context.getMemoryBudget();
}

bool Writer::shouldFlush(const WriterContext& context, size_t nextWriteRows) {
  // TODO: ideally, the heurstics to keep under the memory budget thing
  // shouldn't be a first class concept for writer and should be wrapped in
  // flush policy or some other abstraction for pluggability of the additional
  // logic.

  // If we are hitting memory budget before satisfying flush criteria, try
  // entering low memory mode to work with less memory-intensive encodings.
  bool overBudget = overMemoryBudget(context, nextWriteRows);
  const auto stripeProgress = getStripeProgress(context);
  bool stripeProgressDecision = flushPolicy_->shouldFlush(stripeProgress);
  const auto dictionaryFlushDecision = flushPolicy_->shouldFlushDictionary(
      stripeProgressDecision, overBudget, stripeProgress, context);

  if (FOLLY_UNLIKELY(
          dictionaryFlushDecision == FlushDecision::ABANDON_DICTIONARY)) {
    enterLowMemoryMode();
    // Recalculate memory usage due to encoding switch.
    // We can still be over budget either due to not having enough budget to
    // switch encoding or switching encoding not reducing memory footprint
    // enough.
    overBudget = overMemoryBudget(context, nextWriteRows);
    stripeProgressDecision =
        flushPolicy_->shouldFlush(getStripeProgress(context));
  } else if (dictionaryFlushDecision == FlushDecision::EVALUATE_DICTIONARY) {
    writer_->tryAbandonDictionaries(/*force=*/false);
  }

  const bool shouldFlush = overBudget || stripeProgressDecision ||
      dictionaryFlushDecision == FlushDecision::FLUSH_DICTIONARY;
  if (shouldFlush) {
    VLOG(1) << fmt::format(
        "overMemoryBudget: {}, dictionaryMemUsage: {}, outputStreamSize: {}, generalMemUsage: {}, estimatedStripeSize: {}",
        overBudget,
        context.getMemoryUsage(MemoryUsageCategory::DICTIONARY),
        context.getEstimatedOutputStreamSize(),
        context.getMemoryUsage(MemoryUsageCategory::GENERAL),
        context.getEstimatedStripeSize(context.stripeRawSize()));
  }
  return shouldFlush;
}

void Writer::setLowMemoryMode() {
  writerBase_->getContext().setLowMemoryMode();
}

void Writer::enterLowMemoryMode() {
  auto& context = writerBase_->getContext();
  // Until we have capability to abandon dictionary after the first
  // stripe, do nothing and rely solely on flush to comply with budget.
  if (FOLLY_UNLIKELY(
          context.checkLowMemoryMode() && context.stripeIndex() == 0)) {
    // Idempotent call to switch to less memory intensive encodings.
    writer_->tryAbandonDictionaries(/*force=*/true);
  }
}

void Writer::flushStripe(bool close) {
  auto& context = writerBase_->getContext();
  const int64_t preFlushStreamMemoryUsage =
      context.getMemoryUsage(MemoryUsageCategory::OUTPUT_STREAM);
  if (context.stripeRowCount() == 0) {
    return;
  }

  dwio::common::MetricsLog::StripeFlushMetrics metrics;
  metrics.writerVersion =
      writerVersionToString(context.getConfig(Config::WRITER_VERSION));
  metrics.outputStreamMemoryEstimate = context.getEstimatedOutputStreamSize();
  metrics.stripeSizeEstimate =
      context.getEstimatedStripeSize(context.stripeRawSize());

  if (context.indexEnabled() && context.indexRowCount() > 0) {
    createRowIndexEntry();
  }

  const auto preFlushTotalMemBytes = context.getTotalMemoryUsage();
  ensureStripeFlushFits();
  // NOTE: ensureStripeFlushFits() might trigger memory arbitration that have
  // flushed the current stripe.
  if (context.stripeRowCount() == 0) {
    VELOX_CHECK(canReclaim());
    return;
  }

  TestValue::adjust("facebook::velox::dwrf::Writer::flushStripe", this);

  const auto& handler = context.getEncryptionHandler();
  EncodingManager encodingManager{handler};

  writer_->flush([&](uint32_t nodeId) -> proto::ColumnEncoding& {
    return encodingManager.addEncodingToFooter(nodeId);
  });

  // Collects the memory increment from flushing data to output streams.
  const auto postFlushStreamMemoryUsage =
      context.getMemoryUsage(MemoryUsageCategory::OUTPUT_STREAM);
  const auto flushOverhead =
      postFlushStreamMemoryUsage > preFlushStreamMemoryUsage
      ? postFlushStreamMemoryUsage - preFlushStreamMemoryUsage
      : 0;
  metrics.flushOverhead = static_cast<uint64_t>(flushOverhead);
  context.recordFlushOverhead(metrics.flushOverhead);

  const auto postFlushTotalMemBytes = context.getTotalMemoryUsage();

  auto& sink = writerBase_->getSink();
  auto stripeOffset = sink.size();

  uint32_t lastIndex = 0;
  uint64_t offset = 0;
  const auto addStream = [&](const DwrfStreamIdentifier& stream,
                             const DataBufferHolder& out) {
    uint32_t currentIndex = 0;
    const auto nodeId = stream.encodingKey().node();
    proto::Stream* s = encodingManager.addStreamToFooter(nodeId, currentIndex);

    // set offset only when needed, ie. when offset of current stream cannot be
    // calculated based on offset and length of previous stream. In that case,
    // it must be that current stream and previous stream doesn't belong to same
    // encryption group or neither are encrypted. So the logic is simplified to
    // check if group index are the same for current and previous stream
    if (offset > 0 && lastIndex != currentIndex) {
      s->set_offset(offset);
    }
    lastIndex = currentIndex;

    // Jolly/Presto readers can't read streams bigger than 2GB.
    writerBase_->validateStreamSize(stream, out.size());

    s->set_kind(static_cast<proto::Stream_Kind>(stream.kind()));
    s->set_node(nodeId);
    s->set_column(stream.column());
    s->set_sequence(stream.encodingKey().sequence());
    s->set_length(out.size());
    s->set_usevints(context.getConfig(Config::USE_VINTS));
    offset += out.size();

    context.recordPhysicalSize(stream, out.size());
  };

  // TODO: T45025996 Discard all empty streams at flush time.
  // deals with streams
  uint64_t indexLength = 0;
  sink.setMode(WriterSink::Mode::Index);
  auto result = layoutPlanner_->plan(encodingManager, getStreamList(context));
  result.iterateIndexStreams(
      [&](const DwrfStreamIdentifier& streamId, DataBufferHolder& content) {
        VELOX_CHECK(
            isIndexStream(streamId.kind()),
            "unexpected stream kind {}",
            streamId.kind());
        indexLength += content.size();
        addStream(streamId, content);
        sink.addBuffers(content);
      });

  uint64_t dataLength = 0;
  sink.setMode(WriterSink::Mode::Data);
  result.iterateDataStreams(
      [&](const DwrfStreamIdentifier& streamId, DataBufferHolder& content) {
        VELOX_CHECK(
            !isIndexStream(streamId.kind()),
            "unexpected stream kind {}",
            streamId.kind());
        dataLength += content.size();
        addStream(streamId, content);
        sink.addBuffers(content);
      });
  VELOX_CHECK_GT(dataLength, 0);
  metrics.stripeSize = dataLength;

  if (handler.isEncrypted()) {
    // fill encryption metadata
    for (uint32_t i = 0; i < handler.getEncryptionGroupCount(); ++i) {
      auto group = encodingManager.addEncryptionGroupToFooter();
      writerBase_->writeProtoAsString(
          *group,
          encodingManager.getEncryptionGroup(i),
          std::addressof(handler.getEncryptionProviderByIndex(i)));
    }
  }

  // flush footer
  const uint64_t footerOffset = sink.size();
  VELOX_CHECK_EQ(footerOffset, stripeOffset + dataLength + indexLength);

  sink.setMode(WriterSink::Mode::Footer);
  writerBase_->writeProto(encodingManager.getFooter());
  sink.setMode(WriterSink::Mode::None);

  auto& stripe = writerBase_->addStripeInfo();
  stripe.set_offset(stripeOffset);
  stripe.set_indexlength(indexLength);
  stripe.set_datalength(dataLength);
  stripe.set_footerlength(sink.size() - footerOffset);

  // set encryption key metadata
  if (handler.isEncrypted() && context.stripeIndex() == 0) {
    for (uint32_t i = 0; i < handler.getEncryptionGroupCount(); ++i) {
      *stripe.add_keymetadata() =
          handler.getEncryptionProviderByIndex(i).getKey();
    }
  }

  context.recordAverageRowSize();
  context.recordCompressionRatio(dataLength);

  const auto totalMemoryUsage = context.getTotalMemoryUsage();
  metrics.limit = totalMemoryUsage;
  metrics.availableMemory = context.getMemoryBudget() - totalMemoryUsage;

  auto& dictionaryPool = context.getMemoryPool(MemoryUsageCategory::DICTIONARY);
  metrics.dictionaryMemory = dictionaryPool.usedBytes();
  // TODO: what does this try to capture?
  metrics.maxDictSize = dictionaryPool.stats().peakBytes;

  metrics.stripeIndex = context.stripeIndex();
  metrics.rawStripeSize = context.stripeRawSize();
  metrics.rowsInStripe = context.stripeRowCount();
  metrics.compressionRatio = context.getCompressionRatio();
  metrics.flushOverheadRatio = context.getFlushOverheadRatio();
  metrics.averageRowSize = context.getAverageRowSize();
  metrics.groupSize = 0;
  metrics.close = close;

  VLOG(1) << fmt::format(
      "Stripe {}: Flush overhead = {}, data length = {}, pre flush mem = {}, post flush mem = {}. Closing = {}",
      metrics.stripeIndex,
      metrics.flushOverhead,
      metrics.stripeSize,
      preFlushTotalMemBytes,
      postFlushTotalMemBytes,
      metrics.close);
  addThreadLocalRuntimeStat(
      "stripeSize",
      RuntimeCounter(metrics.stripeSize, RuntimeCounter::Unit::kBytes));
  // Add flush overhead and other ratio logging.
  context.metricLogger()->logStripeFlush(metrics);

  // prepare for next stripe
  context.nextStripe();
  writer_->reset();
}

void Writer::flushInternal(bool close) {
  TestValue::adjust("facebook::velox::dwrf::Writer::flushInternal", this);
  auto exitGuard = folly::makeGuard([this]() { releaseMemory(); });

  auto& context = writerBase_->getContext();
  auto& footer = writerBase_->getFooter();
  auto& sink = writerBase_->getSink();
  {
    CpuWallTimer timer{context.flushTiming()};
    flushStripe(close);

    // if this is the last stripe, add footer
    if (close) {
      const auto& handler = context.getEncryptionHandler();
      std::vector<std::vector<proto::FileStatistics>> stats;
      proto::Encryption* encryption = nullptr;

      // initialize encryption related metadata only when there is data written
      if (handler.isEncrypted() && footer.stripes_size() > 0) {
        const auto count = handler.getEncryptionGroupCount();
        stats.resize(count);
        encryption = footer.mutable_encryption();
        encryption->set_keyprovider(
            encryption::toProto(handler.getKeyProviderType()));
        for (uint32_t i = 0; i < count; ++i) {
          encryption->add_encryptiongroups();
        }
      }

      std::optional<uint32_t> lastRoot;
      std::unordered_map<proto::ColumnStatistics*, proto::ColumnStatistics*>
          statsMap;
      writer_->writeFileStats([&](uint32_t nodeId) -> proto::ColumnStatistics& {
        auto entry = footer.add_statistics();
        if (!encryption || !handler.isEncrypted(nodeId)) {
          return *entry;
        }

        auto root = handler.getEncryptionRoot(nodeId);
        auto groupIndex = handler.getEncryptionGroupIndex(nodeId);
        auto& group = stats.at(groupIndex);
        if (!lastRoot || root != lastRoot.value()) {
          // this is a new root, add to the footer, and use a new slot
          group.emplace_back();
          encryption->mutable_encryptiongroups(groupIndex)->add_nodes(root);
        }
        lastRoot = root;
        auto encryptedStats = group.back().add_statistics();
        statsMap[entry] = encryptedStats;
        return *encryptedStats;
      });

#define COPY_STAT(from, to, stat) \
  if (from->has_##stat()) {       \
    to->set_##stat(from->stat()); \
  }

      // fill basic stats
      for (auto& pair : statsMap) {
        COPY_STAT(pair.second, pair.first, numberofvalues);
        COPY_STAT(pair.second, pair.first, hasnull);
        COPY_STAT(pair.second, pair.first, rawsize);
        COPY_STAT(pair.second, pair.first, size);
      }

#undef COPY_STAT

      // set metadata for each encryption group
      if (encryption) {
        for (uint32_t i = 0; i < handler.getEncryptionGroupCount(); ++i) {
          auto group = encryption->mutable_encryptiongroups(i);
          // set stats. No need to set key metadata since it just reused the
          // same key of the first stripe
          for (auto& s : stats.at(i)) {
            writerBase_->writeProtoAsString(
                *group->add_statistics(),
                s,
                std::addressof(handler.getEncryptionProviderByIndex(i)));
          }
        }
      }

      writerBase_->writeFooter(*schema_->type());
    }

    // flush to sink
    sink.flush();
  }

  if (close) {
    context.metricLogger()->logFileClose(
        dwio::common::MetricsLog::FileCloseMetrics{
            .writerVersion = writerVersionToString(
                context.getConfig(Config::WRITER_VERSION)),
            .footerLength = footer.contentlength(),
            .fileSize = sink.size(),
            .cacheSize = sink.getCacheSize(),
            .numCacheBlocks = sink.getCacheOffsets().size() - 1,
            .cacheMode = static_cast<int32_t>(sink.getCacheMode()),
            .numOfStripes = context.stripeIndex(),
            .rowCount = context.stripeRowCount(),
            .rawDataSize = context.stripeRawSize(),
            .numOfStreams = context.getStreamCount(),
            .totalMemory = context.getTotalMemoryUsage(),
            .dictionaryMemory =
                context.getMemoryUsage(MemoryUsageCategory::DICTIONARY),
            .generalMemory =
                context.getMemoryUsage(MemoryUsageCategory::GENERAL)});
  }
}

void Writer::flush() {
  checkRunning();
  flushInternal(false);
}

void Writer::close() {
  checkRunning();
  auto exitGuard = folly::makeGuard([this]() {
    flushPolicy_->onClose();
    setState(State::kClosed);
  });
  flushInternal(true);
  writerBase_->close();
}

void Writer::abort() {
  checkRunning();
  auto exitGuard = folly::makeGuard([this]() { setState(State::kAborted); });
  // NOTE: we need to reset column writer as all its dependent objects (e.g.
  // writer context) will be reset by writer base abort.
  writer_.reset();
  writerBase_->abort();
}

std::unique_ptr<memory::MemoryReclaimer> Writer::MemoryReclaimer::create(
    Writer* writer) {
  return std::unique_ptr<memory::MemoryReclaimer>(
      new Writer::MemoryReclaimer(writer));
}

bool Writer::MemoryReclaimer::reclaimableBytes(
    const memory::MemoryPool& /*unused*/,
    uint64_t& reclaimableBytes) const {
  reclaimableBytes = 0;
  if (!writer_->canReclaim()) {
    return false;
  }
  TestValue::adjust(
      "facebook::velox::dwrf::Writer::MemoryReclaimer::reclaimableBytes",
      writer_);
  const auto& context = writer_->getContext();
  const auto usedBytes = context.getTotalMemoryUsage();
  const auto releasableBytes = context.releasableMemoryReservation();
  const bool flushable =
      usedBytes >= writer_->spillConfig_->writerFlushThresholdSize;
  if (releasableBytes == 0 && !flushable) {
    return false;
  }
  reclaimableBytes = (flushable ? usedBytes : 0) + releasableBytes;
  return true;
}

uint64_t Writer::MemoryReclaimer::reclaim(
    memory::MemoryPool* pool,
    uint64_t targetBytes,
    uint64_t /*unused*/,
    memory::MemoryReclaimer::Stats& stats) {
  if (!writer_->canReclaim()) {
    return 0;
  }

  if (*writer_->nonReclaimableSection_) {
    RECORD_METRIC_VALUE(kMetricMemoryNonReclaimableCount);
    LOG(WARNING)
        << "Can't reclaim from dwrf writer which is under non-reclaimable "
           "section: "
        << pool->name();
    ++stats.numNonReclaimableAttempts;
    return 0;
  }
  if (!writer_->isRunning()) {
    LOG(WARNING) << "Can't reclaim from a not running dwrf writer: "
                 << pool->name() << ", state: " << writer_->state();
    ++stats.numNonReclaimableAttempts;
    return 0;
  }

  return memory::MemoryReclaimer::run(
      [&]() {
        int64_t reclaimedBytes{0};
        {
          memory::ScopedReclaimedBytesRecorder recorder(pool, &reclaimedBytes);
          const auto& context = writer_->getContext();
          const auto usedBytes = context.getTotalMemoryUsage();
          const auto releasedBytes = writer_->releaseMemory();
          if (releasedBytes == 0 &&
              usedBytes < writer_->spillConfig_->writerFlushThresholdSize) {
            RECORD_METRIC_VALUE(kMetricMemoryNonReclaimableCount);
            LOG(WARNING)
                << "Can't reclaim memory from dwrf writer pool " << pool->name()
                << " which doesn't have sufficient memory to release or flush, "
                   "writer memory usage: "
                << succinctBytes(usedBytes)
                << ", writer memory available reservation: "
                << succinctBytes(context.availableMemoryReservation())
                << ", writer flush memory threshold: "
                << succinctBytes(
                       writer_->spillConfig_->writerFlushThresholdSize);
            ++stats.numNonReclaimableAttempts;
          } else {
            if (usedBytes >= writer_->spillConfig_->writerFlushThresholdSize) {
              writer_->flushInternal(false);
            }
          }
        }
        return reclaimedBytes;
      },
      stats);
}

std::unique_ptr<dwio::common::Writer> DwrfWriterFactory::createWriter(
    std::unique_ptr<dwio::common::FileSink> sink,
    const std::shared_ptr<dwio::common::WriterOptions>& options) {
  auto dwrfOptions = std::dynamic_pointer_cast<dwrf::WriterOptions>(options);
  VELOX_CHECK_NOT_NULL(
      dwrfOptions, "DWRF writer factory expected a DWRF WriterOptions object.");
  return std::make_unique<Writer>(std::move(sink), *dwrfOptions);
}

std::unique_ptr<dwio::common::WriterOptions>
DwrfWriterFactory::createWriterOptions() {
  return std::make_unique<dwrf::WriterOptions>();
}

void WriterOptions::processConfigs(
    const config::ConfigBase& connectorConfig,
    const config::ConfigBase& session) {
  auto dwrfWriterOptions = dynamic_cast<dwrf::WriterOptions*>(this);
  VELOX_CHECK_NOT_NULL(
      dwrfWriterOptions, "Expected a DWRF WriterOptions object.");

  std::map<std::string, std::string> configs = serdeParameters;

  if (compressionKind.has_value()) {
    configs.emplace(
        dwrf::Config::COMPRESSION.key, std::to_string(compressionKind.value()));
  }

  configs.emplace(
      dwrf::Config::STRIPE_SIZE.key,
      std::to_string(orcWriterMaxStripeSize(connectorConfig, session)));

  configs.emplace(
      dwrf::Config::MAX_DICTIONARY_SIZE.key,
      std::to_string(orcWriterMaxDictionaryMemory(connectorConfig, session)));

  configs.emplace(
      dwrf::Config::INTEGER_DICTIONARY_ENCODING_ENABLED.key,
      std::to_string(isOrcWriterIntegerDictionaryEncodingEnabled(
          connectorConfig, session)));
  configs.emplace(
      dwrf::Config::STRING_DICTIONARY_ENCODING_ENABLED.key,
      std::to_string(isOrcWriterStringDictionaryEncodingEnabled(
          connectorConfig, session)));

  configs.emplace(
      dwrf::Config::COMPRESSION_BLOCK_SIZE_MIN.key,
      std::to_string(orcWriterMinCompressionSize(connectorConfig, session)));

  configs.emplace(
      dwrf::Config::LINEAR_STRIPE_SIZE_HEURISTICS.key,
      std::to_string(
          orcWriterLinearStripeSizeHeuristics(connectorConfig, session)));

  configs.emplace(
      dwrf::Config::ZLIB_COMPRESSION_LEVEL.key,
      std::to_string(orcWriterZLIBCompressionLevel(connectorConfig, session)));

  configs.emplace(
      dwrf::Config::ZSTD_COMPRESSION_LEVEL.key,
      std::to_string(orcWriterZSTDCompressionLevel(connectorConfig, session)));

  config = dwrf::Config::fromMap(configs);
}

void registerDwrfWriterFactory() {
  dwio::common::registerWriterFactory(std::make_shared<DwrfWriterFactory>());
}

void unregisterDwrfWriterFactory() {
  dwio::common::unregisterWriterFactory(dwio::common::FileFormat::DWRF);
}

} // namespace facebook::velox::dwrf
