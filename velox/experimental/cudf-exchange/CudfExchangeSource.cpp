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

#include <thread>

#include <cudf/contiguous_split.hpp>
#include <folly/String.h>
#include <folly/Uri.h>
#include "velox/experimental/cudf-exchange/CudfExchangeSource.h"
#include "velox/experimental/cudf/exec/Utilities.h"

using namespace facebook::velox::exec;
namespace facebook::velox::cudf_exchange {

// This constructor is private.
CudfExchangeSource::CudfExchangeSource(
    const std::shared_ptr<Communicator> communicator,
    const std::string& host,
    uint16_t port,
    const PartitionKey& partitionKey,
    const std::shared_ptr<CudfExchangeQueue> queue)
    : CommElement(communicator),
      host_(host),
      port_(port),
      partitionKey_(partitionKey),
      partitionKeyHash_(fnv1a_32(partitionKey_.toString())),
      queue_(std::move(queue)) {
  setState(ReceiverState::Created);
}

/*static*/
std::shared_ptr<CudfExchangeSource> CudfExchangeSource::create(
    const std::string& url,
    const std::shared_ptr<CudfExchangeQueue>& queue) {
  folly::Uri uri(url);
  // Note that there is no distinct schema for the UCXX exchange.
  // The approach is to ignore the schema and not check for HTTP or HTTPS.
  // FIXME: Can't use the HTTP port as this conflicts with Prestissimo!
  // For the time being, there's an ugly hack that just increases the port by 3.
  VLOG(3) << " Creating CudfExchangeSource " << url;
  const std::string host = uri.host();
  int port = uri.port() + 3;
  std::shared_ptr<Communicator> communicator = Communicator::getInstance();
  auto key = extractTaskAndDestinationId(uri.path());
  auto source = std::shared_ptr<CudfExchangeSource>(
      new CudfExchangeSource(communicator, host, port, key, queue));
  // register the exchange source with the communicator. This makes sure that
  // "progress" is called.
  communicator->registerCommElement(source);
  return source;
}

void CudfExchangeSource::process() {
  if (closed_) {
    // Driver thread called closed
    cleanUp();
    return;
  }

  switch (state_) {
    case ReceiverState::Created: {
      // Get the endpoint.
      HostPort hp{host_, port_};
      std::shared_ptr<CudfExchangeSource> selfPtr = getSelfPtr();
      auto epRef = communicator_->assocEndpointRef(selfPtr, hp);
      if (epRef) {
        setEndpoint(epRef);
        setStateIf(
            ReceiverState::Created, ReceiverState::WaitingForHandshakeComplete);
        sendHandshake();
      } else {
        // connection failed.
        VLOG(0) << "Failed to connect to " << host_ << ":"
                << std::to_string(port_);
        setState(ReceiverState::Done);
      }
      communicator_->addToWorkQueue(getSelfPtr());
    } break;
    case ReceiverState::WaitingForHandshakeComplete:
      // Waiting for metadata is handled by an upcall from UCXX. Nothing to do
      break;
    case ReceiverState::ReadyToReceive:
      // change state before calling getMetadata since immediate upcalls in
      // getMetadata will also change state.
      setStateIf(
          ReceiverState::ReadyToReceive, ReceiverState::WaitingForMetadata);
      getMetadata();
      break;
    case ReceiverState::WaitingForMetadata:
      // Waiting for metadata is handled by an upcall from UCXX. Nothing to do
      break;
      break;
    case ReceiverState::WaitingForData:
      // Waiting for data is handled by an upcall from UCXX. Nothing to do.
      break;
    case ReceiverState::Done:
      // We need to call clean-up in this thread to remove any state
      cleanUp();
      break;
  }
}

void CudfExchangeSource::cleanUp() {
  uint32_t value = static_cast<uint32_t>(getState());
  VLOG(3) << "In CudfExchangeSource::cleanUp state == " << value;

  if (!request_->isCompleted()) {
    // The Task has failed and we may need to cancel outstanding requests
    VELOX_CHECK_NOT_NULL(request_);
    request_->cancel();
  }

  if (endpointRef_) {
    endpointRef_->removeCommElem(getSelfPtr());
    endpointRef_ = nullptr;
  }
}

void CudfExchangeSource::close() {
  // This is called by the driver thread so we need ti be careful to
  // indicate to the process thread that we are closing and
  // let it to the actual cleaning up

  bool expected = false;
  bool desired = true;
  if (!closed_.compare_exchange_strong(expected, desired)) {
    return; // already closed.
  }

  VLOG(1) << "CudfExchangeSource::close called.";
  VLOG(1) << fmt::format("closing task: {}", partitionKey_.toString());
  VLOG(3) << "Close receiver to remote " << partitionKey_.toString() << ".";

  //  Let the Communicator progress thread do the actual clean-up
  setState(ReceiverState::Done);
  communicator_->addToWorkQueue(getSelfPtr());
}

folly::F14FastMap<std::string, int64_t> CudfExchangeSource::stats() const {
  VELOX_UNREACHABLE();
}

folly::F14FastMap<std::string, RuntimeMetric> CudfExchangeSource::metrics()
    const {
  folly::F14FastMap<std::string, RuntimeMetric> map;

  // these metrics will be aggregated over all exchange sources of the same
  // exchange client.
  map["cudfExchangeSource.numPackedColumns"] = metrics_.numPackedColumns_;
  map["cudfExchangeSource.totalBytes"] = metrics_.totalBytes_;
  map["cudfExchangeSource.rttPerRequest"] = metrics_.rttPerRequest_;
  return map;
}

// private methods ---
PartitionKey CudfExchangeSource::extractTaskAndDestinationId(
    const std::string& path) {
  // The URL path has the form: /v1/task/<taskId>/results/<destinationId>"
  std::vector<folly::StringPiece> components;
  folly::split('/', path, components, true);

  VELOX_CHECK_EQ(components[0], "v1");
  VELOX_CHECK_EQ(components[1], "task");
  VELOX_CHECK_EQ(components[3], "results");

  uint32_t destinationId;
  try {
    destinationId = static_cast<uint32_t>(std::stoul(components[4].str()));
  } catch (const std::exception& e) {
    std::string msg = "Illegal destination in task URL: " + path;
    VELOX_UNSUPPORTED(msg);
  }

  return PartitionKey{components[2].str(), destinationId};
}

std::shared_ptr<CudfExchangeSource> CudfExchangeSource::getSelfPtr() {
  std::shared_ptr<CudfExchangeSource> ptr;
  try {
    ptr = shared_from_this();
  } catch (std::bad_weak_ptr& exp) {
    ptr = nullptr;
  }
  return ptr;
}

void CudfExchangeSource::enqueue(
    std::unique_ptr<cudf::packed_columns> columns,
    MetadataMsg& metadata) {
  std::vector<velox::ContinuePromise> queuePromises;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());

    queue_->enqueueLocked(std::move(columns), queuePromises);
  }
  // wake up consumers of the CudfExchangeQueue
  for (auto& promise : queuePromises) {
    promise.setValue();
  }
}

void CudfExchangeSource::setEndpoint(std::shared_ptr<EndpointRef> endpointRef) {
  endpointRef_ = std::move(endpointRef);
}

void CudfExchangeSource::sendHandshake() {
  std::shared_ptr<HandshakeMsg> handshakeReq = std::make_shared<HandshakeMsg>();
  handshakeReq->destination = partitionKey_.destination;
  strncpy(
      handshakeReq->taskId,
      partitionKey_.taskId.c_str(),
      sizeof(handshakeReq->taskId));

  VLOG(3) << toString() << " Sending handshake with initial value: "
          << partitionKey_.toString() << " to server";

  // Create the handshake which will register client's existence with the server
  ucxx::AmReceiverCallbackInfo info(
      communicator_->kAmCallbackOwner, communicator_->kAmCallbackId);
  request_ = endpointRef_->endpoint_->amSend(
      handshakeReq.get(),
      sizeof(HandshakeMsg),
      UCS_MEMORY_TYPE_HOST,
      info,
      false,
      std::bind(
          &CudfExchangeSource::onHandshake,
          this,
          std::placeholders::_1,
          std::placeholders::_2),
      handshakeReq);
}

void CudfExchangeSource::onHandshake(
    ucs_status_t status,
    std::shared_ptr<void> arg) {
  if (status != UCS_OK) {
    std::string errorMsg = fmt::format(
        "Failed to send handshake to host {}:{}, task {}: {}",
        host_,
        port_,
        partitionKey_.toString(),
        ucs_status_string(status));
    VLOG(0) << errorMsg;
    setState(ReceiverState::Done);
    queue_->setError(errorMsg); // Let the operator know via the queue

  } else {
    VLOG(3) << toString() << "+ onHandshake " << ucs_status_string(status);
    setStateIf(
        ReceiverState::WaitingForHandshakeComplete,
        ReceiverState::ReadyToReceive);
  }
  // more work to do
  communicator_->addToWorkQueue(getSelfPtr());
}

void CudfExchangeSource::getMetadata() {
  uint32_t sizeMetadata = 4096; // shouldn't be a fixed size.
  auto metadataReq = std::make_shared<std::vector<uint8_t>>(sizeMetadata);
  uint64_t metadataTag = getMetadataTag(partitionKeyHash_, sequenceNumber_);

  VLOG(3) << toString()
          << " waiting for metadata for chunk: " << sequenceNumber_
          << " using tag: " << std::hex << metadataTag << std::dec;

  request_ = endpointRef_->endpoint_->tagRecv(
      reinterpret_cast<void*>(metadataReq->data()),
      sizeMetadata,
      ucxx::Tag{metadataTag},
      ucxx::TagMaskFull,
      false,
      std::bind(
          &CudfExchangeSource::onMetadata,
          this,
          std::placeholders::_1,
          std::placeholders::_2),
      metadataReq);
}

void CudfExchangeSource::onMetadata(
    ucs_status_t status,
    std::shared_ptr<void> arg) {
  VLOG(3) << "+ onMetadata " << ucs_status_string(status);

  if (status != UCS_OK) {
    std::string errorMsg = fmt::format(
        "Failed to receive metadata from host {}:{}, task {}: {}",
        host_,
        port_,
        partitionKey_.toString(),
        ucs_status_string(status));
    VLOG(0) << errorMsg;
    setState(ReceiverState::Done);
    queue_->setError(errorMsg); // Let the operator know via the queue
    communicator_->addToWorkQueue(getSelfPtr());
  } else {
    VELOX_CHECK(arg != nullptr, "Didn't get metadata");

    // arg contains the actual serialized metadata, deserialize the metadata
    std::shared_ptr<std::vector<uint8_t>> metadataMsg =
        std::static_pointer_cast<std::vector<uint8_t>>(arg);

    auto ptr = std::make_shared<DataAndMetadata>();

    ptr->metadata =
        std::move(MetadataMsg::deserializeMetadataMsg(metadataMsg->data()));
    // auto m = std::unique_ptr<MetadataMsg>(new MetadataMsg(msg));

    VLOG(3) << "Datasize bytes == " << ptr->metadata.dataSizeBytes;

    if (ptr->metadata.atEnd) {
      // It seems that all data has been transferred
      atEnd_ = true;
      // enqueue a nullpointer to mark the end for this source.
      VLOG(3) << "There is no more data to transfer for " << toString();
      setStateIf(ReceiverState::WaitingForMetadata, ReceiverState::Done);
      communicator_->addToWorkQueue(getSelfPtr());
      enqueue(nullptr, ptr->metadata);
      // jump out of this function.
      return;
    }

    // Now allocate memory for the CudaVector
    // Get a stream from the global stream pool
    auto stream =
        facebook::velox::cudf_velox::cudfGlobalStreamPool().get_stream();
    try {
      ptr->dataBuf = std::make_unique<rmm::device_buffer>(
          ptr->metadata.dataSizeBytes, stream);
    } catch (const rmm::bad_alloc& e) {
      VLOG(0) << "*** RMM  failed to allocate: " << e.what();
      queue_->setError(
          "Failed to alloc GPU memory"); // Let the operator know via the queue
      setState(ReceiverState::Done);
      communicator_->addToWorkQueue(getSelfPtr());
      return;
    }

    // sync after allocating.
    stream.synchronize();

    VLOG(3) << "Allocated " << ptr->metadata.dataSizeBytes
            << " bytes of device memory";

    // Initiate the transfer of the actual data from GPU-2-GPU
    uint64_t dataTag = getDataTag(partitionKeyHash_, sequenceNumber_);
    VLOG(3) << toString() << " waiting for data for chunk: " << sequenceNumber_
            << " using tag: " << std::hex << dataTag << std::dec;

    if (!setStateIf(
            ReceiverState::WaitingForMetadata, ReceiverState::WaitingForData)) {
      VLOG(1) << "onMetadata Invalid previous state ";
      return;
    }
    request_ = endpointRef_->endpoint_->tagRecv(
        ptr->dataBuf->data(),
        ptr->metadata.dataSizeBytes,
        ucxx::Tag{dataTag},
        ucxx::TagMaskFull,
        false,
        std::bind(
            &CudfExchangeSource::onData,
            this,
            std::placeholders::_1,
            std::placeholders::_2),
        ptr // DataAndMetadata
    );
  }
}

void CudfExchangeSource::onData(
    ucs_status_t status,
    std::shared_ptr<void> arg) {
  VLOG(3) << "+ onData " << ucs_status_string(status);

  if (status != UCS_OK) {
    std::string errorMsg = fmt::format(
        "Failed to receive data from host {}:{}, task {}: {}",
        host_,
        port_,
        partitionKey_.toString(),
        ucs_status_string(status));
    VLOG(0) << toString() << errorMsg;
    setState(ReceiverState::Done);
    queue_->setError(errorMsg); // Let the operator know via the queue
  } else {
    VLOG(3) << toString() << "+ onData " << ucs_status_string(status)
            << " got chunk: " << sequenceNumber_;

    this->sequenceNumber_++;

    std::shared_ptr<DataAndMetadata> ptr =
        std::static_pointer_cast<DataAndMetadata>(arg);

    metrics_.numPackedColumns_.addValue(1);
    metrics_.totalBytes_.addValue(ptr->metadata.dataSizeBytes);

    std::unique_ptr<cudf::packed_columns> columns =
        std::make_unique<cudf::packed_columns>(
            std::move(ptr->metadata.cudfMetadata), std::move(ptr->dataBuf));
    enqueue(std::move(columns), ptr->metadata);
    setStateIf(ReceiverState::WaitingForData, ReceiverState::ReadyToReceive);
  }
  communicator_->addToWorkQueue(getSelfPtr());
}

bool CudfExchangeSource::setStateIf(
    CudfExchangeSource::ReceiverState expected,
    CudfExchangeSource::ReceiverState desired) {
  ReceiverState exp = expected;
  // since spurious failures can happen even if state_ == expected, we need
  // to do this in a loop.
  while (!state_.compare_exchange_strong(
      exp, desired, std::memory_order_acq_rel, std::memory_order_relaxed)) {
    if (exp != expected) {
      // no spurious failure, state isn't what we've expected.
      return false;
    }
    // spurious failure.
    exp = expected; // reset for the next try
  }
  return true;
}

} // namespace facebook::velox::cudf_exchange
