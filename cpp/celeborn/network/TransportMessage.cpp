#include "celeborn/network/TransportMessage.h"
#include "celeborn/utils/Exceptions.h"

namespace celeborn {
TransportMessage::TransportMessage(MessageType type, std::string&& payload)
    : type_(type), payload_(std::move(payload)) {
  messageTypeValue_ = type;
}

TransportMessage::TransportMessage(std::unique_ptr<ReadOnlyByteBuffer> buf) {
  int messageTypeValue = buf->read<int32_t>();
  int payloadLen = buf->read<int32_t>();
  CELEBORN_CHECK_EQ(buf->remainingSize(), payloadLen);
  CELEBORN_CHECK(MessageType_IsValid(messageTypeValue));
  type_ = static_cast<MessageType>(messageTypeValue);
  messageTypeValue_ = type_;
  payload_ = buf->readToString(payloadLen);
}

std::unique_ptr<ReadOnlyByteBuffer> TransportMessage::toReadOnlyByteBuffer()
    const {
  int bufSize = payload_.size() + 4 + 4;
  auto buffer = ByteBuffer::createWriteOnly(bufSize);
  buffer->write<int>(messageTypeValue_);
  buffer->write<int>(payload_.size());
  buffer->writeFromString(payload_);
  CELEBORN_CHECK_EQ(buffer->size(), bufSize);
  return ByteBuffer::toReadOnly(std::move(buffer));
}
} // namespace celeborn