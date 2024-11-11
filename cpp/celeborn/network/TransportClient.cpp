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

#include "celeborn/network/TransportClient.h"

#include "celeborn/network/MessageDecoder.h"
#include "celeborn/network/TransportMessage.h"

namespace celeborn {
void ClientSerializeHandler::read(
    Context* ctx,
    std::unique_ptr<folly::IOBuf> msg) {
  auto buffer = ByteBuffer::createReadOnly(std::move(msg));
  ctx->fireRead(Message::decodeFrom(std::move(buffer)));
}

folly::Future<folly::Unit> ClientSerializeHandler::write(
    Context* ctx,
    std::unique_ptr<Message> msg) {
  // TODO: currently we hand-craft the write procedure of RpcRequest.
  // must be refactored when we support more messages.
  CELEBORN_CHECK(
      msg->type() == Message::RPC_REQUEST, "msgType should be RPC_REQUEST");
  std::unique_ptr<RpcRequest> request(
      reinterpret_cast<RpcRequest*>(msg.release()));

  return ctx->fireWrite(request->encode()->getData());
}

void RpcClientDispatcher::read(Context*, std::unique_ptr<Message> toRecvMsg) {
  switch (toRecvMsg->type()) {
    case Message::RPC_RESPONSE: {
      RpcResponse* response = reinterpret_cast<RpcResponse*>(toRecvMsg.get());
      bool found = true;
      auto holder = requestIdRegistry_.withLock([&](auto& registry) {
        auto search = registry.find(response->requestId());
        if (search == registry.end()) {
          LOG(WARNING)
              << "requestId " << response->requestId()
              << " not found when handling RPC_RESPONSE. Might be outdated already, ignored.";
          found = false;
          return MsgPromiseHolder{};
        }
        auto result = std::move(search->second);
        registry.erase(response->requestId());
        return std::move(result);
      });
      if (found) {
        holder.msgPromise.setValue(std::move(toRecvMsg));
      }
      return;
    }
    case Message::RPC_FAILURE: {
      RpcFailure* failure = reinterpret_cast<RpcFailure*>(toRecvMsg.get());
      bool found = true;
      auto holder = requestIdRegistry_.withLock([&](auto& registry) {
        auto search = registry.find(failure->requestId());
        if (search == registry.end()) {
          LOG(WARNING)
              << "requestId " << failure->requestId()
              << " not found when handling RPC_FAILURE. Might be outdated already, ignored.";
          found = false;
          return MsgPromiseHolder{};
        }
        auto result = std::move(search->second);
        registry.erase(failure->requestId());
        return std::move(result);
      });
      LOG(ERROR) << "Rpc failed, requestId: " << failure->requestId()
                 << " errorMsg: " << failure->errorMsg() << std::endl;
      if (found) {
        holder.msgPromise.setException(
            folly::exception_wrapper(std::exception()));
      }
      return;
    }
    case Message::CHUNK_FETCH_SUCCESS: {
      ChunkFetchSuccess* success =
          reinterpret_cast<ChunkFetchSuccess*>(toRecvMsg.get());
      auto streamChunkSlice = success->streamChunkSlice();
      bool found = true;
      auto holder = streamChunkSliceRegistry_.withLock([&](auto& registry) {
        auto search = registry.find(streamChunkSlice);
        if (search == registry.end()) {
          LOG(WARNING)
              << "streamChunkSlice " << streamChunkSlice.toString()
              << " not found when handling CHUNK_FETCH_SUCCESS. Might be outdated already, ignored.";
          found = false;
          return MsgPromiseHolder{};
        }
        auto result = std::move(search->second);
        registry.erase(streamChunkSlice);
        return std::move(result);
      });
      if (found) {
        holder.msgPromise.setValue(std::move(toRecvMsg));
      }
      return;
    }
    case Message::CHUNK_FETCH_FAILURE: {
      ChunkFetchFailure* failure =
          reinterpret_cast<ChunkFetchFailure*>(toRecvMsg.get());
      auto streamChunkSlice = failure->streamChunkSlice();
      bool found = true;
      auto holder = streamChunkSliceRegistry_.withLock([&](auto& registry) {
        auto search = registry.find(streamChunkSlice);
        if (search == registry.end()) {
          LOG(WARNING)
              << "streamChunkSlice " << streamChunkSlice.toString()
              << " not found when handling CHUNK_FETCH_FAILURE. Might be outdated already, ignored.";
          found = false;
          return MsgPromiseHolder{};
        }
        auto result = std::move(search->second);
        registry.erase(streamChunkSlice);
        return std::move(result);
      });
      std::string errorMsg = fmt::format(
          "fetchChunk failed, streamChunkSlice: {}, errorMsg: {}",
          streamChunkSlice.toString(),
          failure->errorMsg());
      LOG(ERROR) << errorMsg;
      if (found) {
        holder.msgPromise.setException(
            folly::exception_wrapper(std::exception()));
      }
      return;
    }
    default: {
      LOG(ERROR) << "unsupported msg for dispatcher";
    }
  }
}

folly::Future<std::unique_ptr<Message>> RpcClientDispatcher::operator()(
    std::unique_ptr<Message> toSendMsg) {
  CELEBORN_CHECK(!closed_);
  CELEBORN_CHECK(toSendMsg->type() == Message::RPC_REQUEST);
  RpcRequest* request = reinterpret_cast<RpcRequest*>(toSendMsg.get());
  auto f = requestIdRegistry_.withLock(
      [&](auto& registry) -> folly::Future<std::unique_ptr<Message>> {
        auto& holder = registry[request->requestId()];
        holder.requestTime = std::chrono::system_clock::now();
        auto& p = holder.msgPromise;
        p.setInterruptHandler([requestId = request->requestId(),
                               this](const folly::exception_wrapper&) {
          this->requestIdRegistry_.lock()->erase(requestId);
          LOG(WARNING) << "rpc request interrupted, requestId: " << requestId;
        });
        return p.getFuture();
      });

  this->pipeline_->write(std::move(toSendMsg));

  CELEBORN_CHECK(!closed_);
  return f;
}

folly::Future<std::unique_ptr<Message>>
RpcClientDispatcher::sendFetchChunkRequest(
    const StreamChunkSlice& streamChunkSlice,
    std::unique_ptr<Message> toSendMsg) {
  CELEBORN_CHECK(!closed_);
  CELEBORN_CHECK(toSendMsg->type() == Message::RPC_REQUEST);
  auto f = streamChunkSliceRegistry_.withLock([&](auto& registry) {
    auto& holder = registry[streamChunkSlice];
    holder.requestTime = std::chrono::system_clock::now();
    auto& p = holder.msgPromise;
    p.setInterruptHandler(
        [streamChunkSlice, this](const folly::exception_wrapper&) {
          LOG(WARNING) << "fetchChunk request interrupted, "
                          "streamChunkSlice: "
                       << streamChunkSlice.toString();
          this->streamChunkSliceRegistry_.lock()->erase(streamChunkSlice);
        });
    return p.getFuture();
  });
  this->pipeline_->write(std::move(toSendMsg));
  CELEBORN_CHECK(!closed_);
  return f;
}

void RpcClientDispatcher::sendRpcRequestWithoutResponse(
    std::unique_ptr<Message> toSendMsg) {
  CELEBORN_CHECK(toSendMsg->type() == Message::RPC_REQUEST);
  this->pipeline_->write(std::move(toSendMsg));
}

void RpcClientDispatcher::readEOF(Context* ctx) {
  LOG(ERROR) << "readEOF, start to close client";
  ctx->fireReadEOF();
  close();
}

void RpcClientDispatcher::readException(
    Context* ctx,
    folly::exception_wrapper e) {
  LOG(ERROR) << "readException: " << e.what() << " , start to close client";
  ctx->fireReadException(std::move(e));
  close();
}

void RpcClientDispatcher::transportActive(Context* ctx) {
  // Typically do nothing.
  ctx->fireTransportActive();
}

void RpcClientDispatcher::transportInactive(Context* ctx) {
  LOG(ERROR) << "transportInactive, start to close client";
  ctx->fireTransportInactive();
  close();
}

folly::Future<folly::Unit> RpcClientDispatcher::writeException(
    Context* ctx,
    folly::exception_wrapper e) {
  LOG(ERROR) << "writeException: " << e.what() << " , start to close client";
  auto result = ctx->fireWriteException(std::move(e));
  close();
  return result;
}

folly::Future<folly::Unit> RpcClientDispatcher::close() {
  if (!closed_) {
    closed_ = true;
    cleanup();
  }
  return ClientDispatcherBase::close();
}

folly::Future<folly::Unit> RpcClientDispatcher::close(Context* ctx) {
  if (!closed_) {
    closed_ = true;
    cleanup();
  }

  return ClientDispatcherBase::close(ctx);
}

void RpcClientDispatcher::cleanup() {
  LOG(WARNING) << "Cleaning up client!";
  requestIdRegistry_.withLock([&](auto& registry) {
    for (auto& [requestId, promiseHolder] : registry) {
      auto errorMsg =
          fmt::format("Client closed, cancel ongoing requestId {}", requestId);
      LOG(WARNING) << errorMsg;
      promiseHolder.msgPromise.setException(std::runtime_error(errorMsg));
    }
    registry.clear();
  });
  streamChunkSliceRegistry_.withLock([&](auto& registry) {
    for (auto& [streamChunkSlice, promiseHolder] : registry) {
      auto errorMsg = fmt::format(
          "Client closed, cancel ongoing streamChunkSlice {}",
          streamChunkSlice.toString());
      LOG(WARNING) << errorMsg;
      promiseHolder.msgPromise.setException(std::runtime_error(errorMsg));
    }
    registry.clear();
  });
}

SerializePipeline::Ptr MessagePipelineFactory::newPipeline(
    std::shared_ptr<folly::AsyncTransport> sock) {
  auto pipeline = SerializePipeline::create();
  pipeline->addBack(wangle::AsyncSocketHandler(sock));
  // Ensure we can write from any thread.
  pipeline->addBack(wangle::EventBaseHandler());
  pipeline->addBack(MessageDecoder());
  pipeline->addBack(ClientSerializeHandler());
  pipeline->finalize();

  return pipeline;
}

TransportClient::TransportClient(
    std::unique_ptr<wangle::ClientBootstrap<SerializePipeline>> client,
    std::unique_ptr<RpcClientDispatcher> dispatcher,
    Timeout defaultTimeout)
    : client_(std::move(client)),
      dispatcher_(std::move(dispatcher)),
      defaultTimeout_(defaultTimeout) {}

RpcResponse TransportClient::sendRpcRequestSync(
    const RpcRequest& request,
    Timeout timeout) {
  try {
    auto requestMsg = std::make_unique<RpcRequest>(request);
    auto future = (*dispatcher_)(std::move(requestMsg));
    auto responseMsg = std::move(future).get(timeout);
    CELEBORN_CHECK(
        responseMsg->type() == Message::RPC_RESPONSE,
        "responseMsg type should be RPC_RESPONSE");
    return RpcResponse(*reinterpret_cast<RpcResponse*>(responseMsg.get()));
  } catch (const std::exception& e) {
    std::string errorMsg = fmt::format(
        "sendRpc failure, requestId: {}, timeout: {}, errorMsg: {}",
        request.requestId(),
        timeout,
        folly::exceptionStr(e).toStdString());
    LOG(ERROR) << errorMsg;
    CELEBORN_FAIL(errorMsg);
  }
}

void TransportClient::sendRpcRequestWithoutResponse(const RpcRequest& request) {
  try {
    auto requestMsg = std::make_unique<RpcRequest>(request);
    dispatcher_->sendRpcRequestWithoutResponse(std::move(requestMsg));
  } catch (const std::exception& e) {
    std::string errorMsg = fmt::format(
        "sendRpc failure, requestId: {}, errorMsg: {}",
        request.requestId(),
        folly::exceptionStr(e).toStdString());
    LOG(ERROR) << errorMsg;
    CELEBORN_FAIL(errorMsg);
  }
}

std::unique_ptr<ReadOnlyByteBuffer> TransportClient::fetchChunkSync(
    const StreamChunkSlice& streamChunkSlice,
    const RpcRequest& request,
    Timeout timeout) {
  try {
    auto requestMsg = std::make_unique<RpcRequest>(request);
    auto responseMsg =
        dispatcher_
            ->sendFetchChunkRequest(streamChunkSlice, std::move(requestMsg))
            .get(timeout);
    CELEBORN_CHECK(
        responseMsg->type() == Message::CHUNK_FETCH_SUCCESS,
        "responseMsgType should be CHUNK_FETCH_SUCCESS");
    auto chunkFetchSuccess =
        reinterpret_cast<ChunkFetchSuccess*>(responseMsg.get());
    return chunkFetchSuccess->body();
  } catch (const std::exception& e) {
    std::string errorMsg = fmt::format(
        "sendRpc failure, requestId: {}, timeout: {} errorMsg: {}",
        request.requestId(),
        timeout,
        folly::exceptionStr(e).toStdString());
    LOG(ERROR) << errorMsg;
    CELEBORN_FAIL(errorMsg);
  }
}

void TransportClient::fetchChunkAsync(
    const StreamChunkSlice& streamChunkSlice,
    const RpcRequest& request,
    FetchChunkSuccessCallback onSuccess,
    FetchChunkFailureCallback onFailure) {
  try {
    auto requestMsg = std::make_unique<RpcRequest>(request);
    auto future = dispatcher_->sendFetchChunkRequest(
        streamChunkSlice, std::move(requestMsg));
    std::move(future)
        .thenValue([=, _onSuccess = onSuccess, _onFailure = onFailure](
                       std::unique_ptr<Message> responseMsg) {
          if (responseMsg->type() == Message::CHUNK_FETCH_SUCCESS) {
            auto chunkFetchSuccess =
                reinterpret_cast<ChunkFetchSuccess*>(responseMsg.get());
            _onSuccess(streamChunkSlice, chunkFetchSuccess->body());
          } else {
            _onFailure(
                streamChunkSlice,
                std::make_unique<std::runtime_error>(fmt::format(
                    "chunk fetch of streamChunkSlice {} does not succeed",
                    streamChunkSlice.toString())));
          }
        })
        .thenError(
            [=, _onFailure = onFailure](const folly::exception_wrapper& e) {
              _onFailure(
                  streamChunkSlice,
                  std::make_unique<std::runtime_error>(e.what().toStdString()));
            });
  } catch (std::exception& e) {
    CELEBORN_FAIL(e.what());
  }
}

TransportClientFactory::TransportClientFactory(
    const std::shared_ptr<CelebornConf>& conf) {
  google::SetCommandLineOption("GLOG_minloglevel", "0");
  numConnectionsPerPeer_ = conf->networkIoNumConnectionsPerPeer();
  rpcLookupTimeout_ = conf->rpcLookupTimeout();
  connectTimeout_ = conf->networkConnectTimeout();
  numClientThreads_ = conf->networkIoClientThreads();
  if (numClientThreads_ <= 0) {
    numClientThreads_ = std::thread::hardware_concurrency() * 2;
  }
  clientExecutor_ =
      std::make_shared<folly::IOThreadPoolExecutor>(numClientThreads_);
}

std::shared_ptr<TransportClient> TransportClientFactory::createClient(
    const std::string& host,
    uint16_t port) {
  auto address = folly::SocketAddress(host, port);
  auto pool = clientPools_.withLock([&](auto& registry) {
    auto iter = registry.find(address);
    if (iter != registry.end()) {
      return iter->second;
    }
    auto createdPool = std::make_shared<ClientPool>();
    createdPool->clients.resize(numConnectionsPerPeer_);
    registry[address] = createdPool;
    return createdPool;
  });
  auto clientId = std::rand() % numConnectionsPerPeer_;
  {
    std::lock_guard<std::mutex> lock(pool->mutex);
    // TODO: auto-disconnect if the connection is idle for a long time?
    if (pool->clients[clientId] && pool->clients[clientId]->active()) {
      LOG(INFO) << "reusing client for address host " << host << " port "
                << port;
      return pool->clients[clientId];
    }

    auto bootstrap =
        std::make_unique<wangle::ClientBootstrap<SerializePipeline>>();
    bootstrap->group(clientExecutor_);
    bootstrap->pipelineFactory(std::make_shared<MessagePipelineFactory>());
    try {
      auto pipeline = bootstrap->connect(folly::SocketAddress(host, port))
                          .get(rpcLookupTimeout_);

      // TODO: whether to use dispatcher or not should be reconsiderred
      auto dispatcher = std::make_unique<RpcClientDispatcher>();
      dispatcher->setPipeline(pipeline);
      pool->clients[clientId] = std::make_shared<TransportClient>(
          std::move(bootstrap), std::move(dispatcher), connectTimeout_);
      return pool->clients[clientId];
    } catch (const std::exception& e) {
      std::string errorMsg = fmt::format(
          "connect to server failure, serverAddr: {}:{}, timeout: {}, errorMsg: {}",
          host,
          port,
          connectTimeout_,
          folly::exceptionStr(e).toStdString());
      LOG(ERROR) << errorMsg;
      CELEBORN_FAIL(errorMsg);
    }
  }
}
} // namespace celeborn
