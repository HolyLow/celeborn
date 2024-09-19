#pragma once

#include <cstdint>

#include "celeborn/memory/ByteBuffer.h"
#include "celeborn/protocol/ControlMessages.h"
#include "celeborn/utils/Exceptions.h"

namespace celeborn {
/// the Message  = RpcRequest, RpcResponse, ... is the direct object that
/// decoded/encoded by the java's netty encode/decode stack. The message is then
/// decoded again to get application-level objects.
class Message {
 public:
  enum Type {
    UNKNOWN_TYPE = -1,
    CHUNK_FETCH_REQUEST = 0,
    CHUNK_FETCH_SUCCESS = 1,
    CHUNK_FETCH_FAILURE = 2,
    RPC_REQUEST = 3,
    RPC_RESPONSE = 4,
    RPC_FAILURE = 5,
    OPEN_STREAM = 6,
    STREAM_HANDLE = 7,
    ONE_WAY_MESSAGE = 9,
    PUSH_DATA = 11,
    PUSH_MERGED_DATA = 12,
    REGION_START = 13,
    REGION_FINISH = 14,
    PUSH_DATA_HAND_SHAKE = 15,
    READ_ADD_CREDIT = 16,
    READ_DATA = 17,
    OPEN_STREAM_WITH_CREDIT = 18,
    BACKLOG_ANNOUNCEMENT = 19,
    TRANSPORTABLE_ERROR = 20,
    BUFFER_STREAM_END = 21,
    HEARTBEAT = 22,
  };

  static Type decodeType(uint8_t typeId);

  Message(Type type, std::unique_ptr<ReadOnlyByteBuffer>&& body)
      : type_(type), body_(std::move(body)) {}

  Type type() const {
    return type_;
  }

  std::unique_ptr<ReadOnlyByteBuffer> body() const {
    return body_->clone();
  }

  static std::unique_ptr<Message> decodeFrom(
      std::unique_ptr<ReadOnlyByteBuffer>&& data);

 protected:
  Type type_;
  std::unique_ptr<ReadOnlyByteBuffer> body_;
};

class RpcRequest : public Message {
  // TODO: add decode method when required
 public:
  RpcRequest(long requestId, std::unique_ptr<ReadOnlyByteBuffer>&& buf)
      : Message(Type::RPC_REQUEST, std::move(buf)), requestId_(requestId) {}

  RpcRequest(const RpcRequest& other)
      : Message(RPC_REQUEST, other.body_->clone()),
        requestId_(other.requestId_) {}

  long requestId() const {
    return requestId_;
  }

  std::unique_ptr<ReadOnlyByteBuffer> encode() const;

  static long nextRequestId() {
    return currRequestId_.fetch_add(1);
  }

 private:
  long requestId_;
  static std::atomic<long> currRequestId_;
};

class RpcResponse : public Message {
  // TODO: add decode method when required
 public:
  RpcResponse(long requestId, std::unique_ptr<ReadOnlyByteBuffer>&& body)
      : Message(RPC_RESPONSE, std::move(body)), requestId_(requestId) {}

  RpcResponse(const RpcResponse& lhs)
      : Message(RPC_RESPONSE, lhs.body_->clone()), requestId_(lhs.requestId_) {}

  void operator=(const RpcResponse& lhs) {
    requestId_ = lhs.requestId();
    body_ = lhs.body_->clone();
  }

  long requestId() const {
    return requestId_;
  }

  static std::unique_ptr<RpcResponse> decodeFrom(
      std::unique_ptr<ReadOnlyByteBuffer>&& data);

 private:
  long requestId_;
};

class RpcFailure : public Message {
 public:
  RpcFailure(long requestId, std::string&& errorString)
      : Message(RPC_FAILURE, ReadOnlyByteBuffer::createEmptyBuffer()),
        requestId_(requestId),
        errorString_(std::move(errorString)) {}

  RpcFailure(const RpcFailure& other)
      : Message(RPC_FAILURE, ReadOnlyByteBuffer::createEmptyBuffer()),
        requestId_(other.requestId_),
        errorString_(other.errorString_) {}

  long requestId() const {
    return requestId_;
  }

  std::string errorMsg() const {
    return errorString_;
  }

  static std::unique_ptr<RpcFailure> decodeFrom(
      std::unique_ptr<ReadOnlyByteBuffer>&& data);

 private:
  long requestId_;
  std::string errorString_;
};

class ChunkFetchSuccess : public Message {
 public:
  ChunkFetchSuccess(
      StreamChunkSlice& streamChunkSlice,
      std::unique_ptr<ReadOnlyByteBuffer>&& body)
      : Message(CHUNK_FETCH_SUCCESS, std::move(body)),
        streamChunkSlice_(streamChunkSlice) {}

  ChunkFetchSuccess(const ChunkFetchSuccess& other)
      : Message(CHUNK_FETCH_SUCCESS, other.body_->clone()),
        streamChunkSlice_(other.streamChunkSlice_) {}

  static std::unique_ptr<ChunkFetchSuccess> decodeFrom(
      std::unique_ptr<ReadOnlyByteBuffer>&& data);

  StreamChunkSlice streamChunkSlice() const {
    return streamChunkSlice_;
  }

 private:
  StreamChunkSlice streamChunkSlice_;
};

class ChunkFetchFailure : public Message {
 public:
  ChunkFetchFailure(
      StreamChunkSlice& streamChunkSlice,
      std::string&& errorString)
      : Message(CHUNK_FETCH_FAILURE, ReadOnlyByteBuffer::createEmptyBuffer()),
        streamChunkSlice_(streamChunkSlice),
        errorString_(std::move(errorString)) {}

  ChunkFetchFailure(const ChunkFetchFailure& other)
      : Message(CHUNK_FETCH_FAILURE, ReadOnlyByteBuffer::createEmptyBuffer()),
        streamChunkSlice_(other.streamChunkSlice_),
        errorString_(other.errorString_) {}

  static std::unique_ptr<ChunkFetchFailure> decodeFrom(
      std::unique_ptr<ReadOnlyByteBuffer>&& data);

  StreamChunkSlice streamChunkSlice() const {
    return streamChunkSlice_;
  }

  std::string errorMsg() const {
    return errorString_;
  }

 private:
  StreamChunkSlice streamChunkSlice_;
  std::string errorString_;
};
} // namespace celeborn
