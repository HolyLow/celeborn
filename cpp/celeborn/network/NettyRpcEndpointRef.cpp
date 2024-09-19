#include "celeborn/network/NettyRpcEndpointRef.h"

namespace celeborn {

NettyRpcEndpointRef::NettyRpcEndpointRef(
      const std::string& name,
      const std::string& srcHost,
      int srcPort,
      const std::string& dstHost,
      int dstPort,
      const std::shared_ptr<TransportClient>& client)
      : name_(name),
        srcHost_(srcHost),
        srcPort_(srcPort),
        dstHost_(dstHost),
        dstPort_(dstPort),
        client_(client) {}

std::unique_ptr<GetReducerFileGroupResponse> NettyRpcEndpointRef::askSync(
      const GetReducerFileGroup& msg,
      Timeout timeout) {
  auto rpcRequest = buildRpcRequest(msg);
  auto rpcResponse = client_->sendRpcRequestSync(rpcRequest, timeout);
  return fromRpcResponse(std::move(rpcResponse));
}

RpcRequest NettyRpcEndpointRef::buildRpcRequest(const GetReducerFileGroup& msg) {
  auto transportData = msg.toTransportMessage().toReadOnlyByteBuffer();
  int size = srcHost_.size() + 3 + 4 + dstHost_.size() + 3 + 4 +
      name_.size() + 2 + 1;
  auto buffer = ByteBuffer::createWriteOnly(size);
  // write srcAddr msg
  writeRpcAddress(*buffer, srcHost_, srcPort_);
  // write dstAddr msg
  writeRpcAddress(*buffer, dstHost_, dstPort_);
  // write srcName
  writeUTF(*buffer, name_);
  // write the isTransportMessage flag
  buffer->write<uint8_t>(kIsTransportMessageFlag);
  CELEBORN_CHECK_EQ(buffer->size(), size);
  auto result = ByteBuffer::toReadOnly(std::move(buffer));
  auto combined = ByteBuffer::concat(*result, *transportData);
  return RpcRequest(RpcRequest::nextRequestId(), std::move(combined));
}

std::unique_ptr<GetReducerFileGroupResponse> NettyRpcEndpointRef::fromRpcResponse(
      RpcResponse&& response) {
  auto body = response.body();
  bool isTransportMessage = body->read<uint8_t>();
  CELEBORN_CHECK(isTransportMessage);
  auto transportMessage = TransportMessage(std::move(body));
  return GetReducerFileGroupResponse::fromTransportMessage(transportMessage);
}

} // namespace celeborn