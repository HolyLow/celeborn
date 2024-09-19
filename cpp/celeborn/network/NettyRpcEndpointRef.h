#pragma once

#include "celeborn/network/TransportClient.h"
#include "celeborn/protocol/ControlMessages.h"

namespace celeborn {
/// RpcEndpointRef is typically used to communicate with LifecycleManager. It
/// wraps around the TransportClient, add ip and name info to the message as the
/// LifecycleManager requires the information.
class NettyRpcEndpointRef {
 public:
  NettyRpcEndpointRef(
      const std::string& name,
      const std::string& srcHost,
      int srcPort,
      const std::string& dstHost,
      int dstPort,
      const std::shared_ptr<TransportClient>& client);

  std::unique_ptr<GetReducerFileGroupResponse> askSync(
      const GetReducerFileGroup& msg,
      Timeout timeout);

 private:
  RpcRequest buildRpcRequest(const GetReducerFileGroup& msg);

  std::unique_ptr<GetReducerFileGroupResponse> fromRpcResponse(
      RpcResponse&& response);

  std::string name_;
  std::string srcHost_;
  int srcPort_;
  std::string dstHost_;
  int dstPort_;
  std::shared_ptr<TransportClient> client_;

  static constexpr uint8_t kIsTransportMessageFlag = 0xFF;
};
} // namespace celeborn