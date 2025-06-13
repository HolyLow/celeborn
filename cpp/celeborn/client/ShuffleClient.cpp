/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "celeborn/client/ShuffleClient.h"

#include "celeborn/utils/CelebornUtils.h"

namespace celeborn {
namespace client {

std::shared_ptr<PushDataCallback> PushDataCallback::create(
    int shuffleId,
    int mapId,
    int attemptId,
    int partitionId,
    int numMappers,
    int numPartitions,
    const std::string& mapKey,
    int batchId,
    std::unique_ptr<memory::ReadOnlyByteBuffer> databody,
    std::shared_ptr<PushState> pushState,
    std::weak_ptr<ShuffleClientImpl> weakClient,
    int remainingReviveTimes,
    std::shared_ptr<const protocol::PartitionLocation> latestLocation) {
  return std::shared_ptr<PushDataCallback>(new PushDataCallback(
      shuffleId,
      mapId,
      attemptId,
      partitionId,
      numMappers,
      numPartitions,
      mapKey,
      batchId,
      std::move(databody),
      pushState,
      weakClient,
      remainingReviveTimes,
      latestLocation));
}

PushDataCallback::PushDataCallback(
    int shuffleId,
    int mapId,
    int attemptId,
    int partitionId,
    int numMappers,
    int numPartitions,
    const std::string& mapKey,
    int batchId,
    std::unique_ptr<memory::ReadOnlyByteBuffer> databody,
    std::shared_ptr<PushState> pushState,
    std::weak_ptr<ShuffleClientImpl> weakClient,
    int remainingReviveTimes,
    std::shared_ptr<const protocol::PartitionLocation> latestLocation)
    : shuffleId_(shuffleId),
      mapId_(mapId),
      attemptId_(attemptId),
      partitionId_(partitionId),
      numMappers_(numMappers),
      numPartitions_(numPartitions),
      mapKey_(mapKey),
      batchId_(batchId),
      databody_(std::move(databody)),
      pushState_(pushState),
      weakClient_(weakClient),
      remainingReviveTimes_(remainingReviveTimes),
      latestLocation_(latestLocation) {}

void PushDataCallback::onSuccess(
    std::unique_ptr<memory::ReadOnlyByteBuffer> response) {
  auto sharedClient = weakClient_.lock();
  if (!sharedClient) {
    LOG(WARNING) << "ShuffleClientImpl has expired when "
                    "PushDataCallbackOnSuccess, ignored, shuffle "
                 << shuffleId_ << " map " << mapId_ << " attempt " << attemptId_
                 << " partition " << partitionId_ << " batch " << batchId_
                 << ".";
    return;
  }
  if (response->remainingSize() <= 0) {
    pushState_->onSuccess(latestLocation_->hostAndPushPort());
    pushState_->removeBatch(batchId_, latestLocation_->hostAndPushPort());
    return;
  }
  protocol::StatusCode reason =
      static_cast<protocol::StatusCode>(response->read<uint8_t>());
  switch (reason) {
    case protocol::StatusCode::MAP_ENDED: {
      auto mapperEndSet = sharedClient->mapperEndSets_.computeIfAbsent(
          shuffleId_,
          []() { return std::make_shared<utils::ConcurrentHashSet<int>>(); });
      mapperEndSet->insert(mapId_);
      break;
    }
    case protocol::StatusCode::SOFT_SPLIT: {
      VLOG(1) << "Push data to " << latestLocation_->hostAndPushPort()
              << " soft split required for shuffle " << shuffleId_ << " map "
              << mapId_ << " attempt " << attemptId_ << " partition "
              << partitionId_ << " batch " << batchId_ << ".";
      if (!ShuffleClientImpl::newerPartitionLocationExists(
              sharedClient->partitionLocationMaps_.get(shuffleId_).value(),
              partitionId_,
              latestLocation_->epoch)) {
        auto reviveRequest = std::make_shared<protocol::ReviveRequest>(
            shuffleId_,
            mapId_,
            attemptId_,
            partitionId_,
            latestLocation_->epoch,
            latestLocation_,
            protocol::StatusCode::SOFT_SPLIT);
        sharedClient->reviveManager_->addRequest(reviveRequest);
      }
      pushState_->onSuccess(latestLocation_->hostAndPushPort());
      pushState_->removeBatch(batchId_, latestLocation_->hostAndPushPort());
      break;
    }
    case protocol::StatusCode::HARD_SPLIT: {
      VLOG(1) << "Push data to " << latestLocation_->hostAndPushPort()
              << " hard split required for shuffle " << shuffleId_ << " map "
              << mapId_ << " attempt " << attemptId_ << " partition "
              << partitionId_ << " batch " << batchId_ << ".";
      reviveAndRetryPushData(*sharedClient, protocol::StatusCode::HARD_SPLIT);
      break;
    }
    case protocol::StatusCode::PUSH_DATA_SUCCESS_PRIMARY_CONGESTED: {
      VLOG(1) << "Push data to " << latestLocation_->hostAndPushPort()
              << " primary congestion required for shuffle " << shuffleId_
              << " map " << mapId_ << " attempt " << attemptId_ << " partition "
              << partitionId_ << " batch " << batchId_ << ".";
      pushState_->onCongestControl(latestLocation_->hostAndPushPort());
      pushState_->removeBatch(batchId_, latestLocation_->hostAndPushPort());
      break;
    }
    case protocol::StatusCode::PUSH_DATA_SUCCESS_REPLICA_CONGESTED: {
      VLOG(1) << "Push data to " << latestLocation_->hostAndPushPort()
              << " primary congestion required for shuffle " << shuffleId_
              << " map " << mapId_ << " attempt " << attemptId_ << " partition "
              << partitionId_ << " batch " << batchId_ << ".";
      pushState_->onCongestControl(latestLocation_->hostAndPushPort());
      pushState_->removeBatch(batchId_, latestLocation_->hostAndPushPort());
      break;
    }
    default: {
      // This is treated as success.
      LOG(WARNING) << "unhandled PushData success protocol::StatusCode: "
                   << reason;
    }
  }
}

void PushDataCallback::onFailure(std::unique_ptr<std::exception> exception) {
  auto sharedClient = weakClient_.lock();
  if (!sharedClient) {
    LOG(WARNING) << "ShuffleClientImpl has expired when "
                    "PushDataCallbackOnFailure, ignored, shuffle "
                 << shuffleId_ << " map " << mapId_ << " attempt " << attemptId_
                 << " partition " << partitionId_ << " batch " << batchId_
                 << ".";
    return;
  }
  if (pushState_->exceptionExists()) {
    return;
  }

  LOG(ERROR) << "Push data to " << latestLocation_->hostAndPushPort()
             << " failed for shuffle " << shuffleId_ << " map " << mapId_
             << " attempt " << attemptId_ << " partition " << partitionId_
             << " batch " << batchId_ << ", remain revive times "
             << remainingReviveTimes_;

  if (remainingReviveTimes_ <= 0) {
    // TODO: set more specific exception.
    pushState_->setException(std::move(exception));
    return;
  }

  if (sharedClient->mapperEnded(shuffleId_, mapId_)) {
    pushState_->removeBatch(batchId_, latestLocation_->hostAndPushPort());
    LOG(INFO) << "Push data to " << latestLocation_->hostAndPushPort()
              << " failed but mapper already ended for shuffle " << shuffleId_
              << " map " << mapId_ << " attempt " << attemptId_ << " partition "
              << partitionId_ << " batch " << batchId_
              << ", remain revive times " << remainingReviveTimes_ << ".";
    return;
  }
  remainingReviveTimes_--;
  // TODO: the cause should be extracted from error msg...
  protocol::StatusCode cause =
      protocol::StatusCode::PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY;
  reviveAndRetryPushData(*sharedClient, cause);
}

void PushDataCallback::updateLatestLocation(
    std::shared_ptr<const protocol::PartitionLocation> latestLocation) {
  pushState_->addBatch(batchId_, latestLocation->hostAndPushPort());
  pushState_->removeBatch(batchId_, latestLocation_->hostAndPushPort());
  latestLocation_ = latestLocation;
}

void PushDataCallback::reviveAndRetryPushData(
    ShuffleClientImpl& shuffleClient,
    protocol::StatusCode cause) {
  auto reviveRequest = std::make_shared<protocol::ReviveRequest>(
      shuffleId_,
      mapId_,
      attemptId_,
      partitionId_,
      latestLocation_->epoch,
      latestLocation_,
      cause);
  VLOG(1) << "addRequest to reviveManager, shuffleId "
          << reviveRequest->shuffleId << " mapId " << reviveRequest->mapId
          << " attemptId " << reviveRequest->attemptId << " partitionId "
          << reviveRequest->partitionId << " batchId " << batchId_ << " epoch "
          << reviveRequest->epoch;
  shuffleClient.reviveManager_->addRequest(reviveRequest);
  long dueTimeMs = utils::currentTimeMillis() +
      shuffleClient.conf_->clientRpcRequestPartitionLocationRpcAskTimeout() /
          utils::MS(1);
  shuffleClient.pushDataRetryPool_->add(
      [weakClient = this->weakClient_,
       shuffleId = this->shuffleId_,
       body = this->databody_->clone(),
       batchId = this->batchId_,
       callback = shared_from_this(),
       pushState = this->pushState_,
       reviveRequest,
       remainingReviveTimes = this->remainingReviveTimes_,
       dueTimeMs]() {
        auto sharedClient = weakClient.lock();
        if (!sharedClient) {
          LOG(WARNING) << "ShuffleClientImpl has expired when "
                          "PushDataFailureCallback, ignored, shuffleId "
                       << shuffleId;
          return;
        }
        sharedClient->submitRetryPushData(
            shuffleId,
            body->clone(),
            batchId,
            callback,
            pushState,
            reviveRequest,
            remainingReviveTimes,
            dueTimeMs);
      });
}

folly::FunctionScheduler ReviveManager::globalExecutor_ =
    folly::FunctionScheduler();

std::shared_ptr<ReviveManager> ReviveManager::create(
    const std::string& name,
    const conf::CelebornConf& conf,
    std::weak_ptr<ShuffleClientImpl> weakClient) {
  return std::shared_ptr<ReviveManager>(
      new ReviveManager(name, conf, weakClient));
}

ReviveManager::ReviveManager(
    const std::string& name,
    const conf::CelebornConf& conf,
    std::weak_ptr<ShuffleClientImpl> weakClient)
    : name_(name),
      batchSize_(conf.clientPushReviveBatchSize()),
      interval_(conf.clientPushReviveInterval()),
      weakClient_(weakClient) {}

ReviveManager::~ReviveManager() {
  globalExecutor_.cancelFunction(name_);
}

void ReviveManager::start() {
  bool expected = false;
  if (!started_.compare_exchange_strong(expected, true)) {
    return;
  }
  globalExecutor_.start();
  auto task = [weak_this = weak_from_this(), batchSize = batchSize_]() {
    try {
      auto shared_this = weak_this.lock();
      if (!shared_this) {
        return;
      }
      bool continueFlag = true;
      do {
        std::unordered_map<
            long,
            std::unique_ptr<std::unordered_set<PtrReviveRequest>>>
            shuffleMap;
        std::vector<PtrReviveRequest> requests;
        shared_this->requestQueue_.withLock([&](auto& queue) {
          for (int i = 0; i < batchSize && !queue.empty(); i++) {
            requests.push_back(queue.front());
            queue.pop();
          }
        });
        if (requests.empty()) {
          break;
        }
        for (auto& request : requests) {
          auto& set = shuffleMap[request->shuffleId];
          if (!set) {
            set = std::make_unique<std::unordered_set<PtrReviveRequest>>();
          }
          set->insert(request);
        }
        auto shuffleClient = shared_this->weakClient_.lock();
        if (!shuffleClient) {
          return;
        }
        for (auto& [shuffleId, requestSet] : shuffleMap) {
          std::unordered_set<int> mapIds;
          std::vector<PtrReviveRequest> filteredRequests;
          std::unordered_map<int, PtrReviveRequest> requestsToSend;

          auto locationMapOptional =
              shuffleClient->partitionLocationMaps_.get(shuffleId);
          CELEBORN_CHECK(locationMapOptional.has_value());
          auto locationMap = locationMapOptional.value();
          for (auto& request : *requestSet) {
            if (shuffleClient->newerPartitionLocationExists(
                    locationMap, request->partitionId, request->epoch) ||
                shuffleClient->mapperEnded(shuffleId, request->mapId)) {
              request->reviveStatus = protocol::StatusCode::SUCCESS;
            } else {
              filteredRequests.push_back(request);
              mapIds.insert(request->mapId);
              if (auto iter = requestsToSend.find(request->partitionId);
                  iter == requestsToSend.end() ||
                  iter->second->epoch < request->epoch) {
                requestsToSend[request->partitionId] = request;
              }
            }
          }

          if (requestsToSend.empty()) {
            continue;
          }
          if (auto resultOptional =
                  shuffleClient->reviveBatch(shuffleId, mapIds, requestsToSend);
              resultOptional.has_value()) {
            auto result = resultOptional.value();
            for (auto& request : filteredRequests) {
              if (shuffleClient->mapperEnded(shuffleId, request->mapId)) {
                request->reviveStatus = protocol::StatusCode::SUCCESS;
              } else {
                request->reviveStatus = result[request->partitionId];
              }
            }
          } else {
            for (auto& request : filteredRequests) {
              request->reviveStatus = protocol::StatusCode::REVIVE_FAILED;
            }
          }
        }
        continueFlag =
            (shared_this->requestQueue_.lock()->size() > batchSize / 2);
      } while (continueFlag);
    } catch (std::exception& e) {
      LOG(ERROR) << "ReviveManager error occurred: " << e.what();
    }
  };
  startFunction(task);
}

void ReviveManager::startFunction(std::function<void()> task) {
  try {
    globalExecutor_.addFunction(task, interval_, name_, interval_);
  } catch (std::exception& e) {
    LOG(ERROR) << "startFunction failed, current function name " << name_
               << ", retry again...";
    name_ += "-";
    name_ += std::to_string(rand() % 10000);
    startFunction(task);
  }
}

void ReviveManager::addRequest(PtrReviveRequest request) {
  requestQueue_.withLock([&](auto& queue) { queue.push(std::move(request)); });
}

ShuffleClientEndpoint::ShuffleClientEndpoint(
    const std::shared_ptr<const conf::CelebornConf> conf,
    const std::shared_ptr<const conf::CelebornConf> metaClientFactoryConf)
    : conf_(conf) {
  pushDataRetryPool_ = std::make_shared<folly::IOThreadPoolExecutor>(
      conf_->clientPushRetryThreads(),
      std::make_shared<folly::NamedThreadFactory>("celeborn-retry-pushdata"));
  dataClientFactory_ = std::make_shared<network::TransportClientFactory>(conf_);
  // TODO: how to define the metaClientFactory might be reconsidered.
  const auto metaConf = (metaClientFactoryConf) ? metaClientFactoryConf : conf_;
  metaClientFactory_ =
      std::make_shared<network::TransportClientFactory>(metaConf);
}

std::shared_ptr<folly::IOThreadPoolExecutor>
ShuffleClientEndpoint::pushDataRetryPool() const {
  return pushDataRetryPool_;
}

std::shared_ptr<network::TransportClientFactory>
ShuffleClientEndpoint::dataClientFactory() const {
  return dataClientFactory_;
}

std::shared_ptr<network::TransportClientFactory>
ShuffleClientEndpoint::metaClientFactory() const {
  return metaClientFactory_;
}

std::shared_ptr<ShuffleClientImpl> ShuffleClientImpl::create(
    const std::string& appUniqueId,
    const std::shared_ptr<const conf::CelebornConf>& conf,
    const ShuffleClientEndpoint& clientEndpoint) {
  return std::shared_ptr<ShuffleClientImpl>(
      new ShuffleClientImpl(appUniqueId, conf, clientEndpoint));
}

ShuffleClientImpl::ShuffleClientImpl(
    const std::string& appUniqueId,
    const std::shared_ptr<const conf::CelebornConf>& conf,
    const ShuffleClientEndpoint& clientEndpoint)
    : appUniqueId_(appUniqueId),
      conf_(conf),
      dataClientFactory_(clientEndpoint.dataClientFactory()),
      metaClientFactory_(clientEndpoint.metaClientFactory()),
      pushDataRetryPool_(clientEndpoint.pushDataRetryPool()) {}

void ShuffleClientImpl::setupLifecycleManagerRef(
    const std::string& host,
    int port) {
  auto managerClient = metaClientFactory_->createClient(host, port);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    lifecycleManagerRef_ = std::make_shared<network::NettyRpcEndpointRef>(
        "LifecycleManagerEndpoint",
        "dummy",
        0,
        host,
        port,
        managerClient,
        *conf_);
    initReviveManagerLocked();
  }
}

void ShuffleClientImpl::setupLifecycleManagerRef(
    std::shared_ptr<network::NettyRpcEndpointRef>& lifecycleManagerRef) {
  std::lock_guard<std::mutex> lock(mutex_);
  lifecycleManagerRef_ = lifecycleManagerRef;
  initReviveManagerLocked();
}

std::shared_ptr<utils::ConcurrentHashMap<
    int,
    std::shared_ptr<const protocol::PartitionLocation>>>
ShuffleClientImpl::getPartitionLocation(
    int shuffleId,
    int numMappers,
    int numPartitions) {
  auto partitionLocationOptional = partitionLocationMaps_.get(shuffleId);
  if (partitionLocationOptional.has_value()) {
    return partitionLocationOptional.value();
  }

  registerShuffle(shuffleId, numMappers, numPartitions);

  partitionLocationOptional = partitionLocationMaps_.get(shuffleId);
  CELEBORN_CHECK(
      partitionLocationOptional.has_value(),
      "partitionLocation is empty because registerShuffle failed");
  auto partitionLocationMap = partitionLocationOptional.value();
  CELEBORN_CHECK_NOT_NULL(partitionLocationMap);
  return partitionLocationMap;
}

int ShuffleClientImpl::pushData(
    int shuffleId,
    int mapId,
    int attemptId,
    int partitionId,
    const uint8_t* data,
    size_t offset,
    size_t length,
    int numMappers,
    int numPartitions) {
  const auto mapKey = utils::makeMapKey(shuffleId, mapId, attemptId);
  if (checkMapperEnded(shuffleId, mapId, mapKey)) {
    return 0;
  }

  auto partitionLocationMap =
      getPartitionLocation(shuffleId, numMappers, numPartitions);
  CELEBORN_CHECK_NOT_NULL(partitionLocationMap);
  auto partitionLocationOptional = partitionLocationMap->get(partitionId);
  if (!partitionLocationOptional.has_value()) {
    if (!revive(
            shuffleId,
            mapId,
            attemptId,
            partitionId,
            -1,
            nullptr,
            protocol::StatusCode::PUSH_DATA_FAIL_NON_CRITICAL_CAUSE)) {
      CELEBORN_FAIL(
          fmt::format(
              "Revive for shuffleId {} partitionId {} failed.",
              shuffleId,
              partitionId));
    }
    partitionLocationOptional = partitionLocationMap->get(partitionId);
  }
  if (checkMapperEnded(shuffleId, mapId, mapKey)) {
    return 0;
  }

  CELEBORN_CHECK(partitionLocationOptional.has_value());
  auto partitionLocation = partitionLocationOptional.value();
  auto pushState = getPushState(mapKey);
  const int nextBatchId = pushState->nextBatchId();

  // TODO: compression is not supported.

  auto writeBuffer =
      memory::ByteBuffer::createWriteOnly(kBatchHeaderSize + length);
  // TODO: the java side uses Platform to write the data. We simply assume
  //  littleEndian here.
  writeBuffer->writeLE<int>(mapId);
  writeBuffer->writeLE<int>(attemptId);
  writeBuffer->writeLE<int>(nextBatchId);
  writeBuffer->writeLE<int>(length);
  writeBuffer->writeFromBuffer(data, offset, length);

  auto hostAndPushPort = partitionLocation->hostAndPushPort();
  // Check limit.
  limitMaxInFlight(mapKey, *pushState, hostAndPushPort);
  // Add inFlight requests.
  pushState->addBatch(nextBatchId, hostAndPushPort);
  // Build pushData request.
  const auto shuffleKey = utils::makeShuffleKey(appUniqueId_, shuffleId);
  auto body = memory::ByteBuffer::toReadOnly(std::move(writeBuffer));
  network::PushData pushData(
      network::Message::nextRequestId(),
      protocol::PartitionLocation::Mode::PRIMARY,
      shuffleKey,
      partitionLocation->uniqueId(),
      body->clone());
  // Build callback.
  auto pushDataCallback = PushDataCallback::create(
      shuffleId,
      mapId,
      attemptId,
      partitionId,
      numMappers,
      numPartitions,
      mapKey,
      nextBatchId,
      body->clone(),
      pushState,
      weak_from_this(),
      conf_->clientPushMaxReviveTimes(),
      partitionLocation);
  // Do push data.
  auto client = dataClientFactory_->createClient(
      partitionLocation->host, partitionLocation->pushPort, partitionId);
  client->pushDataAsync(
      pushData, conf_->clientPushDataTimeout(), pushDataCallback);
  return body->remainingSize();
}

void ShuffleClientImpl::mapperEnd(
    int shuffleId,
    int mapId,
    int attemptId,
    int numMappers) {
  mapPartitionMapperEnd(shuffleId, mapId, attemptId, numMappers, -1);
}

void ShuffleClientImpl::mapPartitionMapperEnd(
    int shuffleId,
    int mapId,
    int attemptId,
    int numMappers,
    int partitionId) {
  auto mapKey = utils::makeMapKey(shuffleId, mapId, attemptId);
  auto pushState = getPushState(mapKey);

  try {
    limitZeroInFlight(mapKey, *pushState);

    auto mapperEndResponse =
        lifecycleManagerRef_
            ->askSync<protocol::MapperEnd, protocol::MapperEndResponse>(
                protocol::MapperEnd{
                    shuffleId, mapId, attemptId, numMappers, partitionId});
    if (mapperEndResponse->status != protocol::StatusCode::SUCCESS) {
      CELEBORN_FAIL(
          "MapperEnd failed. protocol::StatusCode " +
          std::to_string(mapperEndResponse->status));
    }
  } catch (std::exception& e) {
    LOG(ERROR) << "mapperEnd failed, error msg: " << e.what();
    pushStates_.erase(mapKey);
    CELEBORN_FAIL(e.what());
  }
  pushStates_.erase(mapKey);
}

void ShuffleClientImpl::cleanup(int shuffleId, int mapId, int attemptId) {
  auto mapKey = utils::makeMapKey(shuffleId, mapId, attemptId);
  auto pushStateOptional = pushStates_.erase(mapKey);
  if (pushStateOptional.has_value()) {
    auto pushState = pushStateOptional.value();
    pushState->setException(
        std::make_unique<std::runtime_error>(mapKey + "is cleaned up"));
  }
}

std::unique_ptr<CelebornInputStream> ShuffleClientImpl::readPartition(
    int shuffleId,
    int partitionId,
    int attemptNumber,
    int startMapIndex,
    int endMapIndex) {
  const auto reducerFileGroupInfo = getReducerFileGroupInfo(shuffleId);
  CELEBORN_CHECK_NOT_NULL(reducerFileGroupInfo);
  std::string shuffleKey = utils::makeShuffleKey(appUniqueId_, shuffleId);
  std::vector<std::shared_ptr<const protocol::PartitionLocation>> locations;
  if (!reducerFileGroupInfo->fileGroups.empty() &&
      reducerFileGroupInfo->fileGroups.count(partitionId)) {
    locations = std::move(
        utils::toVector(
            reducerFileGroupInfo->fileGroups.find(partitionId)->second));
  }
  return std::make_unique<CelebornInputStream>(
      shuffleKey,
      conf_,
      dataClientFactory_,
      std::move(locations),
      reducerFileGroupInfo->attempts,
      attemptNumber,
      startMapIndex,
      endMapIndex);
}

void ShuffleClientImpl::updateReducerFileGroup(int shuffleId) {
  CELEBORN_CHECK(
      lifecycleManagerRef_, "lifecycleManagerRef_ is not initialized");
  // Send the query request to lifecycleManager.
  auto reducerFileGroupInfo = lifecycleManagerRef_->askSync<
      protocol::GetReducerFileGroup,
      protocol::GetReducerFileGroupResponse>(
      protocol::GetReducerFileGroup{shuffleId},
      conf_->clientRpcGetReducerFileGroupRpcAskTimeout());

  switch (reducerFileGroupInfo->status) {
    case protocol::SUCCESS: {
      VLOG(1) << "success to get reducerFileGroupInfo, shuffleId " << shuffleId;
      reducerFileGroupInfos_.set(
          shuffleId,
          std::shared_ptr<protocol::GetReducerFileGroupResponse>(
              reducerFileGroupInfo.release()));
      return;
    }
    case protocol::SHUFFLE_NOT_REGISTERED: {
      // We cannot treat this as a failure. It indicates this is an empty
      // shuffle.
      LOG(WARNING) << "shuffleId " << shuffleId
                   << " is not registered when get reducerFileGroupInfo";
      reducerFileGroupInfos_.set(
          shuffleId,
          std::shared_ptr<protocol::GetReducerFileGroupResponse>(
              reducerFileGroupInfo.release()));
      return;
    }
    case protocol::STAGE_END_TIME_OUT:
    case protocol::SHUFFLE_DATA_LOST: {
      LOG(ERROR) << "shuffleId " << shuffleId
                 << " failed getReducerFileGroupInfo with code "
                 << reducerFileGroupInfo->status;
      CELEBORN_FAIL("failed protocol::GetReducerFileGroupResponse code");
    }
    default: {
      CELEBORN_FAIL("undefined protocol::GetReducerFileGroupResponse code");
    }
  }
}

bool ShuffleClientImpl::cleanupShuffle(int shuffleId) {
  shuffleMutexes_.erase(shuffleId);
  reducerFileGroupInfos_.erase(shuffleId);
  partitionLocationMaps_.erase(shuffleId);
  mapperEndSets_.erase(shuffleId);
  stageEndShuffleSet_.erase(shuffleId);

  LOG(INFO) << "Unregistered shuffle " << shuffleId;
  return true;
}

std::shared_ptr<PushState> ShuffleClientImpl::getPushState(
    const std::string& mapKey) {
  return pushStates_.computeIfAbsent(
      mapKey, [&]() { return std::make_shared<PushState>(*conf_); });
}

void ShuffleClientImpl::initReviveManagerLocked() {
  if (!reviveManager_) {
    std::string uniqueName = appUniqueId_;
    uniqueName += std::to_string(utils::currentTimeNanos());
    reviveManager_ =
        ReviveManager::create(uniqueName, *conf_, weak_from_this());
    reviveManager_->start();
  }
}

void ShuffleClientImpl::registerShuffle(
    int shuffleId,
    int numMappers,
    int numPartitions) {
  auto shuffleMutex = shuffleMutexes_.computeIfAbsent(
      shuffleId, []() { return std::make_shared<std::mutex>(); });
  // RegisterShuffle might be issued concurrently, we only allow one issue
  // for each shuffleId.
  std::lock_guard<std::mutex> lock(*shuffleMutex);
  if (partitionLocationMaps_.containsKey(shuffleId)) {
    return;
  }
  CELEBORN_CHECK(
      lifecycleManagerRef_, "lifecycleManagerRef_ is not initialized");
  const int maxRetries = conf_->clientRegisterShuffleMaxRetries();
  int numRetries = 1;
  for (; numRetries <= maxRetries; numRetries++) {
    try {
      // Send the query request to lifecycleManager.
      auto registerShuffleResponse = lifecycleManagerRef_->askSync<
          protocol::RegisterShuffle,
          protocol::RegisterShuffleResponse>(
          protocol::RegisterShuffle{shuffleId, numMappers, numPartitions},
          conf_->clientRpcRegisterShuffleRpcAskTimeout());

      switch (registerShuffleResponse->status) {
        case protocol::StatusCode::SUCCESS: {
          VLOG(1) << "success to registerShuffle, shuffleId " << shuffleId
                  << " numMappers " << numMappers << " numPartitions "
                  << numPartitions;
          auto partitionLocationMap = std::make_shared<utils::ConcurrentHashMap<
              int,
              std::shared_ptr<const protocol::PartitionLocation>>>();
          auto& partitionLocations =
              registerShuffleResponse->partitionLocations;
          for (int i = 0; i < partitionLocations.size(); i++) {
            auto id = partitionLocations[i]->id;
            partitionLocationMap->set(id, std::move(partitionLocations[i]));
          }
          partitionLocationMaps_.set(
              shuffleId, std::move(partitionLocationMap));
          return;
        }
        default: {
          LOG(ERROR)
              << "LifecycleManager request slots return protocol::StatusCode "
              << registerShuffleResponse->status << " , shuffleId " << shuffleId
              << " NumMappers " << numMappers << " numPartitions "
              << numPartitions << " , retry again, remain retry times "
              << maxRetries - numRetries;
        }
      }
    } catch (std::exception& e) {
      CELEBORN_FAIL(
          fmt::format(
              "registerShuffle encounters error after {} tries, "
              "shuffleId {} numMappers {} numPartitions {}, errorMsg: {}",
              numRetries,
              shuffleId,
              numMappers,
              numPartitions,
              e.what()));
      break;
    }
    std::this_thread::sleep_for(conf_->clientRegisterShuffleRetryWait());
  }
  partitionLocationMaps_.set(shuffleId, nullptr);
  CELEBORN_FAIL(
      fmt::format(
          "registerShuffle failed after {} tries, "
          "shuffleId {} numMappers {} numPartitions {}",
          maxRetries,
          shuffleId,
          numMappers,
          numPartitions));
}

void ShuffleClientImpl::submitRetryPushData(
    int shuffleId,
    std::unique_ptr<memory::ReadOnlyByteBuffer> body,
    int batchId,
    std::shared_ptr<PushDataCallback> pushDataCallback,
    std::shared_ptr<PushState> pushState,
    PtrReviveRequest request,
    int remainReviveTimes,
    long dueTimeMs) {
  long reviveWaitTimeMs = dueTimeMs - utils::currentTimeMillis();
  long accumulatedTimeMs = 0;
  const long deltaMs = 50;
  while (request->reviveStatus.load() ==
             protocol::StatusCode::REVIVE_INITIALIZED &&
         accumulatedTimeMs <= reviveWaitTimeMs) {
    std::this_thread::sleep_for(utils::MS(deltaMs));
    accumulatedTimeMs += deltaMs;
  }
  if (mapperEnded(shuffleId, request->mapId)) {
    VLOG(1) << "Revive for push data success, but the mapper already ended "
               "for shuffle "
            << shuffleId << " map " << request->mapId << " attempt "
            << request->attemptId << " partition " << request->partitionId
            << " batch " << batchId << " location hostAndPushPort "
            << request->loc->hostAndPushPort() << ".";
    pushState->removeBatch(batchId, request->loc->hostAndPushPort());
    return;
  }
  if (request->reviveStatus.load() != protocol::StatusCode::SUCCESS) {
    // TODO: the exception message here should be assembled...
    pushDataCallback->onFailure(std::make_unique<std::exception>());
    return;
  }
  auto locationMapOptional = partitionLocationMaps_.get(shuffleId);
  CELEBORN_CHECK(locationMapOptional.has_value());
  auto newLocationOptional =
      locationMapOptional.value()->get(request->partitionId);
  CELEBORN_CHECK(newLocationOptional.has_value());
  auto newLocation = newLocationOptional.value();
  LOG(INFO) << "Revive for push data success, new location for shuffle "
            << shuffleId << " map " << request->mapId << " attempt "
            << request->attemptId << " partition " << request->partitionId
            << " batch " << batchId << " is location hostAndPushPort "
            << newLocation->hostAndPushPort() << ".";
  pushDataCallback->updateLatestLocation(newLocation);

  try {
    CELEBORN_CHECK_GT(remainReviveTimes, 0, "no remainReviveTime left");
    network::PushData pushData(
        network::Message::nextRequestId(),
        protocol::PartitionLocation::Mode::PRIMARY,
        utils::makeShuffleKey(appUniqueId_, shuffleId),
        newLocation->uniqueId(),
        std::move(body));
    auto client = dataClientFactory_->createClient(
        newLocation->host, newLocation->pushPort, request->partitionId);
    client->pushDataAsync(
        pushData, conf_->clientPushDataTimeout(), pushDataCallback);
  } catch (std::exception e) {
    LOG(ERROR) << "Exception raised while pushing data for shuffle "
               << shuffleId << " map " << request->mapId << " attempt "
               << request->attemptId << " partition " << request->partitionId
               << " batch " << batchId << " location hostAndPushPort "
               << newLocation->hostAndPushPort() << " errorMsg " << e.what()
               << ".";
    // TODO: the failure should be treated better...
    pushDataCallback->onFailure(std::make_unique<std::exception>(e));
  }
}

bool ShuffleClientImpl::checkMapperEnded(
    int shuffleId,
    int mapId,
    const std::string& mapKey) {
  if (mapperEnded(shuffleId, mapId)) {
    VLOG(1) << "Mapper already ended for shuffle " << shuffleId << " map "
            << mapId;
    if (auto pushStateOptional = pushStates_.get(mapKey);
        pushStateOptional.has_value()) {
      auto pushState = pushStateOptional.value();
      pushState->cleanup();
    }
    return true;
  }
  return false;
}

bool ShuffleClientImpl::mapperEnded(int shuffleId, int mapId) {
  if (auto mapperEndSetOptional = mapperEndSets_.get(shuffleId);
      mapperEndSetOptional.has_value() &&
      mapperEndSetOptional.value()->contains(mapId)) {
    return true;
  }
  if (stageEnded(shuffleId)) {
    return true;
  }
  return false;
}

bool ShuffleClientImpl::stageEnded(int shuffleId) {
  return stageEndShuffleSet_.contains(shuffleId);
}

std::optional<std::unordered_map<int, int>> ShuffleClientImpl::reviveBatch(
    int shuffleId,
    const std::unordered_set<int>& mapIds,
    const std::unordered_map<int, PtrReviveRequest>& requests) {
  std::unordered_map<int, int> result;
  auto partitionLocationMap = partitionLocationMaps_.get(shuffleId).value();
  std::unordered_map<int, std::shared_ptr<const protocol::PartitionLocation>>
      oldLocationMap;
  protocol::Revive revive;
  revive.shuffleId = shuffleId;
  revive.mapIds.insert(mapIds.begin(), mapIds.end());
  for (auto& [partitionId, request] : requests) {
    oldLocationMap[request->partitionId] = request->loc;
    revive.reviveRequests.insert(request);
  }
  try {
    auto response =
        lifecycleManagerRef_
            ->askSync<protocol::Revive, protocol::ChangeLocationResponse>(
                revive,
                conf_->clientRpcRequestPartitionLocationRpcAskTimeout());
    auto mapperEndSet = mapperEndSets_.computeIfAbsent(shuffleId, []() {
      return std::make_shared<utils::ConcurrentHashSet<int>>();
    });
    for (auto endedMapId : response->endedMapIds) {
      mapperEndSet->insert(endedMapId);
    }
    for (auto& partitionInfo : response->partitionInfos) {
      switch (partitionInfo.status) {
        case protocol::StatusCode::SUCCESS: {
          partitionLocationMap->set(
              partitionInfo.partitionId, partitionInfo.partition);
          break;
        }
        case protocol::StatusCode::STAGE_ENDED: {
          stageEndShuffleSet_.insert(shuffleId);
          return {std::move(result)};
        }
        case protocol::StatusCode::SHUFFLE_NOT_REGISTERED: {
          LOG(ERROR) << "shuffleId " << shuffleId << " not registered!";
          return std::nullopt;
        }
        default: {
          // noop
        }
      }
      result[partitionInfo.partitionId] = partitionInfo.status;
    }
    return {std::move(result)};
  } catch (std::exception& e) {
    LOG(ERROR) << "reviveBatch failed: " << e.what();
    return std::nullopt;
  }
}

bool ShuffleClientImpl::revive(
    int shuffleId,
    int mapId,
    int attemptId,
    int partitionId,
    int epoch,
    std::shared_ptr<const protocol::PartitionLocation> oldLocation,
    protocol::StatusCode cause) {
  auto request = std::make_shared<protocol::ReviveRequest>(
      shuffleId, mapId, attemptId, partitionId, epoch, oldLocation, cause);
  auto resultOptional =
      reviveBatch(shuffleId, {mapId}, {{partitionId, request}});
  if (mapperEnded(shuffleId, mapId)) {
    VLOG(1) << "Revive success, but the mapper ended for shuffle " << shuffleId
            << " map " << mapId << " attempt " << attemptId << " partition"
            << partitionId << ", just return true(Assume revive successfully).";
    return true;
  }
  if (resultOptional.has_value()) {
    auto result = resultOptional.value();
    return result.find(partitionId) != result.end() &&
        result[partitionId] == protocol::StatusCode::SUCCESS;
  }
  return false;
}

void ShuffleClientImpl::limitMaxInFlight(
    const std::string& mapKey,
    PushState& pushState,
    const std::string hostAndPushPort) {
  bool reachLimit = pushState.limitMaxInFlight(hostAndPushPort);
  if (reachLimit) {
    auto msg = fmt::format(
        "Waiting timeout for task {} while limiting max "
        "in-flight requests to {}.",
        mapKey,
        hostAndPushPort);
    if (auto exceptionMsgOptional = pushState.getExceptionMsg();
        exceptionMsgOptional.has_value()) {
      msg += " PushState exception: " + exceptionMsgOptional.value();
    }
    CELEBORN_FAIL(msg);
  }
}

void ShuffleClientImpl::limitZeroInFlight(
    const std::string& mapKey,
    PushState& pushState) {
  bool reachLimit = pushState.limitZeroInFlight();
  if (reachLimit) {
    auto msg = fmt::format(
        "Waiting timeout for task {} while limiting zero "
        "in-flight requests.",
        mapKey);
    if (auto exceptionMsgOptional = pushState.getExceptionMsg();
        exceptionMsgOptional.has_value()) {
      msg += " PushState exception: " + exceptionMsgOptional.value();
    }
    CELEBORN_FAIL(msg);
  }
}

bool ShuffleClientImpl::newerPartitionLocationExists(
    std::shared_ptr<utils::ConcurrentHashMap<
        int,
        std::shared_ptr<const protocol::PartitionLocation>>> locationMap,
    int partitionId,
    int epoch) {
  if (auto locationOptional = locationMap->get(partitionId);
      locationOptional.has_value() && locationOptional.value()->epoch > epoch) {
    return true;
  }
  return false;
}

std::shared_ptr<protocol::GetReducerFileGroupResponse>
ShuffleClientImpl::getReducerFileGroupInfo(int shuffleId) {
  auto reducerFileGroupInfoOptional = reducerFileGroupInfos_.get(shuffleId);
  if (reducerFileGroupInfoOptional.has_value()) {
    return reducerFileGroupInfoOptional.value();
  }
  updateReducerFileGroup(shuffleId);
  return getReducerFileGroupInfo(shuffleId);
}
} // namespace client
} // namespace celeborn
