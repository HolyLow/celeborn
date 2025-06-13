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

#include <folly/experimental/FunctionScheduler.h>

#include "celeborn/client/reader/CelebornInputStream.h"
#include "celeborn/client/writer/PushState.h"
#include "celeborn/network/NettyRpcEndpointRef.h"
#include "celeborn/utils/CelebornUtils.h"

namespace celeborn {
namespace client {
class ShuffleClient {
 public:
  virtual void setupLifecycleManagerRef(const std::string& host, int port) = 0;

  virtual void setupLifecycleManagerRef(
      std::shared_ptr<network::NettyRpcEndpointRef>& lifecycleManagerRef) = 0;

  virtual int pushData(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      const uint8_t* data,
      size_t offset,
      size_t length,
      int numMappers,
      int numPartitions) = 0;

  // TODO: support pushData with folly::IOBuf to avoid memcpy.

  virtual void
  mapperEnd(int shuffleId, int mapId, int attemptId, int numMappers) = 0;

  // Cleanup states of a map task.
  virtual void cleanup(int shuffleId, int mapId, int attemptId) = 0;

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

class ShuffleClientImpl;

class PushDataCallback : public network::RpcResponseCallback,
                         public std::enable_shared_from_this<PushDataCallback> {
 public:
  // Only allow construction from create() method to ensure that functionality
  // of std::shared_from_this works properly.
  static std::shared_ptr<PushDataCallback> create(
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
      std::shared_ptr<const protocol::PartitionLocation> latestLocation);

  void onSuccess(std::unique_ptr<memory::ReadOnlyByteBuffer> response) override;

  void onFailure(std::unique_ptr<std::exception> exception) override;

  // The location of a PushDataCallback might be updated if a revive is
  // involved, and the location must be updated by calling
  // updateLatestLocation() to make sure the location is properly updated.
  void updateLatestLocation(
      std::shared_ptr<const protocol::PartitionLocation> latestLocation);

 private:
  // The constructor is hidden to ensure that functionality of
  // std::shared_from_this works properly.
  PushDataCallback(
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
      std::shared_ptr<const protocol::PartitionLocation> latestLocation);

  void reviveAndRetryPushData(
      ShuffleClientImpl& shuffleClient,
      protocol::StatusCode cause);

  const int shuffleId_;
  const int mapId_;
  const int attemptId_;
  const int partitionId_;
  const int numMappers_;
  const int numPartitions_;
  const std::string mapKey_;
  const int batchId_;
  const std::unique_ptr<memory::ReadOnlyByteBuffer> databody_;
  const std::shared_ptr<PushState> pushState_;
  const std::weak_ptr<ShuffleClientImpl> weakClient_;
  int remainingReviveTimes_;
  std::shared_ptr<const protocol::PartitionLocation> latestLocation_;
};

/// ReviveManager is responsible for buffering the ReviveRequests, and issue
/// the revive requests periodically in batches.
class ReviveManager : public std::enable_shared_from_this<ReviveManager> {
 public:
  using PtrReviveRequest = std::shared_ptr<protocol::ReviveRequest>;

  // Only allow construction from create() method to ensure that functionality
  // of std::shared_from_this works properly.
  static std::shared_ptr<ReviveManager> create(
      const std::string& name,
      const conf::CelebornConf& conf,
      std::weak_ptr<ShuffleClientImpl> weakClient);

  ~ReviveManager();

  // The start() method must not be called within constructor, because for
  // std::enable_shred_from_this the shared_from_this() or weak_from_this()
  // would not work until the object construction is complete.
  void start();

  void addRequest(PtrReviveRequest request);

 private:
  // The constructor is hidden to ensure that functionality of
  // std::shared_from_this works properly.
  ReviveManager(
      const std::string& name,
      const conf::CelebornConf& conf,
      std::weak_ptr<ShuffleClientImpl> weakClient);

  void startFunction(std::function<void()> task);

  // Scheduler for issuing the requests periodically.
  static folly::FunctionScheduler globalExecutor_;

  std::string name_;
  const int batchSize_;
  const Timeout interval_;
  std::weak_ptr<ShuffleClientImpl> weakClient_;
  folly::Synchronized<std::queue<PtrReviveRequest>, std::mutex> requestQueue_;
  std::atomic<bool> started_{false};
};

/// ShuffleClientEndpoint holds all the resources of ShuffleClient, including
/// threadPools and clientFactories. The endpoint could be reused by multiple
/// ShuffleClient to avoid creating too many resources.
class ShuffleClientEndpoint {
 public:
  ShuffleClientEndpoint(
      const std::shared_ptr<const conf::CelebornConf> conf,
      const std::shared_ptr<const conf::CelebornConf> metaClientFactoryConf =
          nullptr);

  std::shared_ptr<folly::IOThreadPoolExecutor> pushDataRetryPool() const;

  std::shared_ptr<network::TransportClientFactory> dataClientFactory() const;

  std::shared_ptr<network::TransportClientFactory> metaClientFactory() const;

 private:
  const std::shared_ptr<const conf::CelebornConf> conf_;
  std::shared_ptr<folly::IOThreadPoolExecutor> pushDataRetryPool_;
  std::shared_ptr<network::TransportClientFactory> dataClientFactory_;
  std::shared_ptr<network::TransportClientFactory> metaClientFactory_;
};

class ShuffleClientImpl
  : public ShuffleClient,
  public std::enable_shared_from_this<ShuffleClientImpl> {
 public:
  friend class PushDataCallback;
  friend class ReviveManager;
  using PtrReviveRequest = std::shared_ptr<protocol::ReviveRequest>;

  // Only allow construction from create() method to ensure that functionality
  // of std::shared_from_this works properly.
  static std::shared_ptr<ShuffleClientImpl> create(
      const std::string& appUniqueId,
      const std::shared_ptr<const conf::CelebornConf>& conf,
      const ShuffleClientEndpoint& clientEndpoint);

  void setupLifecycleManagerRef(const std::string& host, int port) override;

  void setupLifecycleManagerRef(std::shared_ptr<network::NettyRpcEndpointRef>&
                                    lifecycleManagerRef) override;

  std::shared_ptr<
      utils::ConcurrentHashMap<int, std::shared_ptr<const protocol::PartitionLocation>>>
  getPartitionLocation(int shuffleId, int numMappers, int numPartitions);

  int pushData(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      const uint8_t* data,
      size_t offset,
      size_t length,
      int numMappers,
      int numPartitions) override;

  void mapperEnd(int shuffleId, int mapId, int attemptId, int numMappers)
      override;

  void mapPartitionMapperEnd(
      int shuffleId,
      int mapId,
      int attemptId,
      int numMappers,
      int partitionId);

  void cleanup(int shuffleId, int mapId, int attemptId) override;

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
  // The constructor is hidden to ensure that functionality of
  // std::shared_from_this works properly.
  ShuffleClientImpl(
      const std::string& appUniqueId,
      const std::shared_ptr<const conf::CelebornConf>& conf,
      const ShuffleClientEndpoint& clientEndpoint);


  std::shared_ptr<PushState> getPushState(const std::string& mapKey);

  void initReviveManagerLocked();

  void registerShuffle(int shuffleId, int numMappers, int numPartitions);

  void submitRetryPushData(
      int shuffleId,
      std::unique_ptr<memory::ReadOnlyByteBuffer> body,
      int batchId,
      std::shared_ptr<PushDataCallback> pushDataCallback,
      std::shared_ptr<PushState> pushState,
      PtrReviveRequest request,
      int remainReviveTimes,
      long dueTimeMs);

  bool checkMapperEnded(int shuffleId, int mapId, const std::string& mapKey);

  bool mapperEnded(int shuffleId, int mapId);

  bool stageEnded(int shuffleId);

  std::optional<std::unordered_map<int, int>> reviveBatch(
      int shuffleId,
      const std::unordered_set<int>& mapIds,
      const std::unordered_map<int, PtrReviveRequest>& requests);

  bool revive(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      int epoch,
      std::shared_ptr<const protocol::PartitionLocation> oldLocation,
      protocol::StatusCode cause);

  // Check if the pushState's ongoing package num reaches the max limit, if so,
  // block until the ongoing package num decreases below max limit.
  void limitMaxInFlight(
      const std::string& mapKey,
      PushState& pushState,
      const std::string hostAndPushPort);

  // Check if the pushState's ongoing package num reaches zero, if not, block
  // until the ongoing package num decreases to zero.
  void limitZeroInFlight(const std::string& mapKey, PushState& pushState);

  // TODO: no support for WAIT as it is not used.
  static bool newerPartitionLocationExists(
      std::shared_ptr<
          utils::ConcurrentHashMap<int, std::shared_ptr<const protocol::PartitionLocation>>>
          locationMap,
      int partitionId,
      int epoch);

  std::shared_ptr<protocol::GetReducerFileGroupResponse> getReducerFileGroupInfo(
      int shuffleId);

  static constexpr size_t kBatchHeaderSize = 4 * 4;

  const std::string appUniqueId_;
  std::shared_ptr<const conf::CelebornConf> conf_;
  std::shared_ptr<network::NettyRpcEndpointRef> lifecycleManagerRef_;
  std::shared_ptr<network::TransportClientFactory> dataClientFactory_;
  std::shared_ptr<network::TransportClientFactory> metaClientFactory_;
  std::shared_ptr<folly::IOExecutor> pushDataRetryPool_;
  std::shared_ptr<ReviveManager> reviveManager_;
  std::mutex mutex_;
  utils::ConcurrentHashMap<int, std::shared_ptr<std::mutex>> shuffleMutexes_;
  utils::ConcurrentHashMap<int, std::shared_ptr<protocol::GetReducerFileGroupResponse>>
        reducerFileGroupInfos_;
  utils::ConcurrentHashMap<
      int,
      std::shared_ptr<
          utils::ConcurrentHashMap<int, std::shared_ptr<const protocol::PartitionLocation>>>>
      partitionLocationMaps_;
  utils::ConcurrentHashMap<std::string, std::shared_ptr<PushState>> pushStates_;
  utils::ConcurrentHashMap<int, std::shared_ptr<utils::ConcurrentHashSet<int>>>
      mapperEndSets_;
  utils::ConcurrentHashSet<int> stageEndShuffleSet_;

  // TODO: pushExcludedWorker is not supported yet
};
} // namespace client
} // namespace celeborn
