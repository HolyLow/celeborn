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

#include <fmt/chrono.h>
#include <folly/io/IOBuf.h>
#include <wangle/bootstrap/ClientBootstrap.h>
#include <wangle/channel/AsyncSocketHandler.h>
#include <wangle/channel/EventBaseHandler.h>
#include <wangle/service/ClientDispatcher.h>

#include "celeborn/conf/CelebornConf.h"
#include "celeborn/network/Message.h"
#include "celeborn/protocol/ControlMessages.h"
#include "celeborn/utils/CelebornUtils.h"

namespace celeborn {
class ClientSerializeHandler : public wangle::Handler<
                                   std::unique_ptr<folly::IOBuf>,
                                   std::unique_ptr<Message>,
                                   std::unique_ptr<Message>,
                                   std::unique_ptr<folly::IOBuf>> {
 public:
  void read(Context* ctx, std::unique_ptr<folly::IOBuf> msg) override;

  folly::Future<folly::Unit> write(Context* ctx, std::unique_ptr<Message> msg)
      override;
};

using SerializePipeline =
    wangle::Pipeline<folly::IOBufQueue&, std::unique_ptr<Message>>;

class RpcClientDispatcher : public wangle::ClientDispatcherBase<
                                SerializePipeline,
                                std::unique_ptr<Message>,
                                std::unique_ptr<Message>> {
 public:
  void read(Context*, std::unique_ptr<Message> toRecvMsg) override;

  folly::Future<std::unique_ptr<Message>> operator()(
      std::unique_ptr<Message> toSendMsg) override;

  folly::Future<std::unique_ptr<Message>> sendFetchChunkRequest(
      const StreamChunkSlice& streamChunkSlice,
      std::unique_ptr<Message> toSendMsg);

  void sendRpcRequestWithoutResponse(std::unique_ptr<Message> toSendMsg);

  void readEOF(Context* ctx) override;

  void readException(Context* ctx, folly::exception_wrapper e) override;

  void transportActive(Context* ctx) override;

  void transportInactive(Context* ctx) override;

  folly::Future<folly::Unit> writeException(
      Context* ctx,
      folly::exception_wrapper e) override;

  folly::Future<folly::Unit> close() override;

  folly::Future<folly::Unit> close(Context* ctx) override;

  bool isAvailable() override {
    return !closed_;
  }

 private:
  void cleanup();

  using MsgPromise = folly::Promise<std::unique_ptr<Message>>;
  struct MsgPromiseHolder {
    MsgPromise msgPromise;
    std::chrono::time_point<std::chrono::system_clock> requestTime;
  };
  folly::Synchronized<std::unordered_map<long, MsgPromiseHolder>, std::mutex>
      requestIdRegistry_;
  folly::Synchronized<
      std::unordered_map<
          StreamChunkSlice,
          MsgPromiseHolder,
          StreamChunkSlice::Hasher>,
      std::mutex>
      streamChunkSliceRegistry_;
  std::atomic<bool> closed_{false};
};

class MessagePipelineFactory
    : public wangle::PipelineFactory<SerializePipeline> {
 public:
  SerializePipeline::Ptr newPipeline(
      std::shared_ptr<folly::AsyncTransport> sock) override;
};

using FetchChunkSuccessCallback = std::function<void(
    StreamChunkSlice streamChunkSlice,
    std::unique_ptr<ReadOnlyByteBuffer>)>;

using FetchChunkFailureCallback = std::function<void(
    StreamChunkSlice streamChunkSlice,
    std::unique_ptr<std::exception> exception)>;

class TransportClient {
 public:
  TransportClient(
      std::unique_ptr<wangle::ClientBootstrap<SerializePipeline>> client,
      std::unique_ptr<RpcClientDispatcher> dispatcher,
      Timeout defaultTimeout);

  RpcResponse sendRpcRequestSync(const RpcRequest& request) {
    return sendRpcRequestSync(request, defaultTimeout_);
  }

  RpcResponse sendRpcRequestSync(const RpcRequest& request, Timeout timeout);

  // Ignore the response, return immediately.
  void sendRpcRequestWithoutResponse(const RpcRequest& request);

  std::unique_ptr<ReadOnlyByteBuffer> fetchChunkSync(
      const StreamChunkSlice& streamChunkSlice,
      const RpcRequest& request) {
    return fetchChunkSync(streamChunkSlice, request, defaultTimeout_);
  }

  std::unique_ptr<ReadOnlyByteBuffer> fetchChunkSync(
      const StreamChunkSlice& streamChunkSlice,
      const RpcRequest& request,
      Timeout timeout);

  void fetchChunkAsync(
      const StreamChunkSlice& streamChunkSlice,
      const RpcRequest& request,
      FetchChunkSuccessCallback onSuccess,
      FetchChunkFailureCallback onFailure);

  bool active() const {
    return dispatcher_->isAvailable();
  }

  ~TransportClient() = default;

 private:
  std::unique_ptr<wangle::ClientBootstrap<SerializePipeline>> client_;
  std::unique_ptr<RpcClientDispatcher> dispatcher_;
  Timeout defaultTimeout_;
};

class TransportClientFactory {
 public:
  TransportClientFactory(const std::shared_ptr<CelebornConf>& conf);

  std::shared_ptr<TransportClient> createClient(
      const std::string& host,
      uint16_t port);

 private:
  struct ClientPool {
    std::mutex mutex;
    std::vector<std::shared_ptr<TransportClient>> clients;
  };

  struct Hasher {
    size_t operator()(const folly::SocketAddress& lhs) const {
      return lhs.hash();
    }
  };

  int numConnectionsPerPeer_;
  folly::Synchronized<
      std::unordered_map<
          folly::SocketAddress,
          std::shared_ptr<ClientPool>,
          Hasher>,
      std::mutex>
      clientPools_;
  Timeout rpcLookupTimeout_;
  Timeout connectTimeout_;
  int numClientThreads_;
  std::shared_ptr<folly::IOThreadPoolExecutor> clientExecutor_;
};
} // namespace celeborn