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

#pragma once

#include "celeborn/network/NettyRpcEndpointRef.h"
#include "celeborn/client/reader/CelebornInputStream.h"

namespace celeborn {
class ShuffleClient {
 public:
  virtual void setupLifecycleManagerRef(std::string& host, int port) = 0;

  virtual void setupLifecycleManagerRef(
      std::shared_ptr<NettyRpcEndpointRef>& lifecycleManagerRef) = 0;

  virtual void updateReducerFileGroup(int shuffleId) = 0;

  virtual std::unique_ptr<CelebornInputStream> readPartition(
      int shuffleId,
      int partitionId,
      int attemptNumber,
      int startMapIndex,
      int endMapIndex) = 0;

  virtual bool cleanupShuffle(int shuffleId) = 0;

  virtual void shutdown() = 0;
};

class ShuffleClientImpl : public ShuffleClient {
 public:
  ShuffleClientImpl(
      const std::string& appUniqueId,
      const std::shared_ptr<const CelebornConf>& conf,
      const std::shared_ptr<TransportClientFactory>& clientFactory);

  void setupLifecycleManagerRef(std::string& host, int port) override;

  void setupLifecycleManagerRef(
      std::shared_ptr<NettyRpcEndpointRef>& lifecycleManagerRef) override;

  std::unique_ptr<CelebornInputStream> readPartition(
      int shuffleId,
      int partitionId,
      int attemptNumber,
      int startMapIndex,
      int endMapIndex) override;

  void updateReducerFileGroup(int shuffleId) override;

  bool cleanupShuffle(int shuffleId) override;

  void shutdown() override {}

 private:
  GetReducerFileGroupResponse& getReducerFileGroupInfo(int shuffleId);

  const std::string appUniqueId_;
  std::shared_ptr<const CelebornConf> conf_;
  std::shared_ptr<NettyRpcEndpointRef> lifecycleManagerRef_;
  std::shared_ptr<TransportClientFactory> clientFactory_;
  std::mutex mutex_;
  std::unordered_map<long, std::unique_ptr<GetReducerFileGroupResponse>>
      reducerFileGroupInfos_;
};
} // namespace celeborn
