#pragma once

#include <folly/io/Cursor.h>
#include <string>

#include "celeborn/memory/ByteBuffer.h"
#include "celeborn/proto/TransportMessagesCpp.pb.h"

namespace celeborn {
class TransportMessage {
 public:
  TransportMessage(MessageType type, std::string&& payload);

  TransportMessage(std::unique_ptr<ReadOnlyByteBuffer> buf);

  std::unique_ptr<ReadOnlyByteBuffer> toReadOnlyByteBuffer() const;

  MessageType type() const {
    return type_;
  }

  std::string payload() const {
    return payload_;
  }

 private:
  MessageType type_;
  int messageTypeValue_;
  std::string payload_;
};
} // namespace celeborn
