#include "celeborn/network/Message.h"

namespace celeborn {
Message::Type Message::decodeType(uint8_t typeId) {
  switch (typeId) {
    case 0:
      return CHUNK_FETCH_REQUEST;
    case 1:
      return CHUNK_FETCH_SUCCESS;
    case 2:
      return CHUNK_FETCH_FAILURE;
    case 3:
      return RPC_REQUEST;
    case 4:
      return RPC_RESPONSE;
    case 5:
      return RPC_FAILURE;
    case 6:
      return OPEN_STREAM;
    case 7:
      return STREAM_HANDLE;
    case 9:
      return ONE_WAY_MESSAGE;
    case 11:
      return PUSH_DATA;
    case 12:
      return PUSH_MERGED_DATA;
    case 13:
      return REGION_START;
    case 14:
      return REGION_FINISH;
    case 15:
      return PUSH_DATA_HAND_SHAKE;
    case 16:
      return READ_ADD_CREDIT;
    case 17:
      return READ_DATA;
    case 18:
      return OPEN_STREAM_WITH_CREDIT;
    case 19:
      return BACKLOG_ANNOUNCEMENT;
    case 20:
      return TRANSPORTABLE_ERROR;
    case 21:
      return BUFFER_STREAM_END;
    case 22:
      return HEARTBEAT;
    default:
      CELEBORN_FAIL("Unknown message type " + std::to_string(typeId));
  }
}

std::atomic<long> RpcRequest::currRequestId_ = 1000;

std::unique_ptr<Message> Message::decodeFrom(
    std::unique_ptr<ReadOnlyByteBuffer>&& data) {
  int32_t encodedLength = data->read<int32_t>();
  uint8_t typeId = data->read<uint8_t>();
  int32_t bodyLength = data->read<int32_t>();
  assert(encodedLength + bodyLength == data->remainingSize());
  Type type = decodeType(typeId);
  switch (type) {
    case RPC_RESPONSE:
      return RpcResponse::decodeFrom(std::move(data));
    case RPC_FAILURE:
      return RpcFailure::decodeFrom(std::move(data));
    case CHUNK_FETCH_SUCCESS:
      return ChunkFetchSuccess::decodeFrom(std::move(data));
    case CHUNK_FETCH_FAILURE:
      return ChunkFetchFailure::decodeFrom(std::move(data));
    default:
      CELEBORN_FAIL("unsupported Message decode type " + std::to_string(type));
  }
}

std::unique_ptr<ReadOnlyByteBuffer> RpcRequest::encode() const {
  int bodyLength = body_->remainingSize();
  int encodedLength = 8 + 4;
  int headerLength = 4 + 1 + 4 + encodedLength;
  auto buffer = ByteBuffer::createWriteOnly(headerLength);
  buffer->write<int32_t>(encodedLength);
  buffer->write<uint8_t>(RPC_REQUEST);
  buffer->write<int32_t>(bodyLength);
  buffer->write<long>(requestId_);
  buffer->write<int32_t>(bodyLength);
  auto result = ByteBuffer::toReadOnly(std::move(buffer));
  auto combined = ByteBuffer::concat(*result, *body_);
  return std::move(combined);
}

std::unique_ptr<RpcResponse> RpcResponse::decodeFrom(
    std::unique_ptr<ReadOnlyByteBuffer>&& data) {
  long requestId = data->read<long>();
  data->skip(4);
  auto result = std::make_unique<RpcResponse>(requestId, std::move(data));
  return result;
}

std::unique_ptr<RpcFailure> RpcFailure::decodeFrom(
    std::unique_ptr<ReadOnlyByteBuffer>&& data) {
  long requestId = data->read<long>();
  int strLen = data->read<int>();
  CELEBORN_CHECK_EQ(data->remainingSize(), strLen);
  std::string errorString = data->readToString(strLen);
  return std::make_unique<RpcFailure>(requestId, std::move(errorString));
}

std::unique_ptr<ChunkFetchSuccess> ChunkFetchSuccess::decodeFrom(
    std::unique_ptr<ReadOnlyByteBuffer>&& data) {
  StreamChunkSlice streamChunkSlice = StreamChunkSlice::decodeFrom(*data);
  return std::make_unique<ChunkFetchSuccess>(streamChunkSlice, std::move(data));
}

std::unique_ptr<ChunkFetchFailure> ChunkFetchFailure::decodeFrom(
    std::unique_ptr<ReadOnlyByteBuffer>&& data) {
  StreamChunkSlice streamChunkSlice = StreamChunkSlice::decodeFrom(*data);
  int strLen = data->read<int>();
  CELEBORN_CHECK_EQ(data->remainingSize(), strLen);
  std::string errorString = data->readToString(strLen);
  return std::make_unique<ChunkFetchFailure>(
      streamChunkSlice, std::move(errorString));
}
} // namespace celeborn
