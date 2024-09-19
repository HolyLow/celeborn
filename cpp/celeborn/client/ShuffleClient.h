#pragma once

#include "celeborn/network/NettyRpcEndpointRef.h"
#include "celeborn/client/reader/CelebornInputStream.h"

namespace celeborn {
class ShuffleClient {
 public:
  virtual void setupLifecycleManagerRef(std::string& host, int port) = 0;

  virtual void setupLifecycleManagerRef(
      std::shared_ptr<NettyRpcEndpointRef>& lifecycleManagerRef) = 0;

  virtual void updateReducerFileGroup(long shuffleId) = 0;

  virtual std::unique_ptr<CelebornInputStream> readPartition(
      long shuffleId,
      int partitionId,
      int attemptNumber,
      int startMapIndex,
      int endMapIndex) = 0;

  virtual bool cleanupShuffle(long shuffleId) = 0;

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
      long shuffleId,
      int partitionId,
      int attemptNumber,
      int startMapIndex,
      int endMapIndex) override;

  void updateReducerFileGroup(long shuffleId) override;

  bool cleanupShuffle(long shuffleId) override;

  void shutdown() override {}

 private:
  GetReducerFileGroupResponse& getReducerFileGroupInfo(long shuffleId);

  const std::string appUniqueId_;
  std::shared_ptr<const CelebornConf> conf_;
  std::shared_ptr<NettyRpcEndpointRef> lifecycleManagerRef_;
  std::shared_ptr<TransportClientFactory> clientFactory_;
  std::mutex mutex_;
  std::unordered_map<long, std::unique_ptr<GetReducerFileGroupResponse>>
      reducerFileGroupInfos_;
};
} // namespace celeborn
