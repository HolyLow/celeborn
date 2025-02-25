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

ShuffleClientImpl::ShuffleClientImpl(
    const std::string& appUniqueId,
    const std::shared_ptr<const CelebornConf>& conf,
    const std::shared_ptr<TransportClientFactory>& clientFactory)
    : appUniqueId_(appUniqueId), conf_(conf), clientFactory_(clientFactory) {}

void ShuffleClientImpl::setupLifecycleManagerRef(std::string& host, int port) {
  auto managerClient = clientFactory_->createClient(host, port);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    lifecycleManagerRef_ = std::make_shared<NettyRpcEndpointRef>(
        "LifecycleManagerEndpoint", "dummy", 0, host, port, managerClient);
  }
}

void ShuffleClientImpl::setupLifecycleManagerRef(
    std::shared_ptr<NettyRpcEndpointRef>& lifecycleManagerRef) {
  std::lock_guard<std::mutex> lock(mutex_);
  lifecycleManagerRef_ = lifecycleManagerRef;
}

std::unique_ptr<CelebornInputStream> ShuffleClientImpl::readPartition(
    int shuffleId,
    int partitionId,
    int attemptNumber,
    int startMapIndex,
    int endMapIndex) {
  const auto& reducerFileGroupInfo = getReducerFileGroupInfo(shuffleId);
  std::string shuffleKey = makeShuffleKey(appUniqueId_, shuffleId);
  std::vector<std::shared_ptr<const PartitionLocation>> locations;
  if (!reducerFileGroupInfo.fileGroups.empty() &&
      reducerFileGroupInfo.fileGroups.count(partitionId)) {
    locations = std::move(
        toVector(reducerFileGroupInfo.fileGroups.find(partitionId)->second));
  }
  return std::make_unique<CelebornInputStream>(
      shuffleKey,
      conf_,
      clientFactory_,
      std::move(locations),
      reducerFileGroupInfo.attempts,
      attemptNumber,
      startMapIndex,
      endMapIndex);
}

void ShuffleClientImpl::updateReducerFileGroup(int shuffleId) {
  CELEBORN_CHECK(
      lifecycleManagerRef_, "lifecycleManagerRef_ is not initialized");
  // Send the query request to lifecycleManager.
  auto reducerFileGroupInfo = lifecycleManagerRef_->askSync(
      GetReducerFileGroup{shuffleId},
      conf_->clientRpcGetReducerFileGroupRpcAskTimeout());

  switch (reducerFileGroupInfo->status) {
    case SUCCESS: {
      VLOG(1) << "success to get reducerFileGroupInfo, shuffleId " << shuffleId;
      std::lock_guard<std::mutex> lock(mutex_);
      reducerFileGroupInfos_[shuffleId] = std::move(reducerFileGroupInfo);
      return;
    }
    case SHUFFLE_NOT_REGISTERED: {
      // We cannot treat this as a failure. It indicates this is an empty
      // shuffle.
      LOG(WARNING) << "shuffleId " << shuffleId
                   << " is not registered when get reducerFileGroupInfo";
      std::lock_guard<std::mutex> lock(mutex_);
      reducerFileGroupInfos_[shuffleId] = std::move(reducerFileGroupInfo);
      return;
    }
    case STAGE_END_TIME_OUT:
    case SHUFFLE_DATA_LOST: {
      LOG(ERROR) << "shuffleId " << shuffleId
                 << " failed getReducerFileGroupInfo with code "
                 << reducerFileGroupInfo->status;
      CELEBORN_FAIL("failed GetReducerFileGroupResponse code");
    }
    default: {
      CELEBORN_FAIL("undefined GetReducerFileGroupResponse code");
    }
  }
}

bool ShuffleClientImpl::cleanupShuffle(int shuffleId) {
  std::lock_guard<std::mutex> lock(mutex_);
  reducerFileGroupInfos_.erase(shuffleId);
  return true;
}

GetReducerFileGroupResponse& ShuffleClientImpl::getReducerFileGroupInfo(
    int shuffleId) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto iter = reducerFileGroupInfos_.find(shuffleId);
    if (iter != reducerFileGroupInfos_.end()) {
      return *iter->second;
    }
  }

  updateReducerFileGroup(shuffleId);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return *reducerFileGroupInfos_[shuffleId];
  }
}

} // namespace celeborn