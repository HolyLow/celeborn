#pragma once

#include <wangle/codec/ByteToMessageDecoder.h>

namespace celeborn {
/**
 * A complete Message encoding/decoding frame is:
 * -----------------------------------------------------------------------
 * | encodedLength | msgType | bodyLength | encodedContent | bodyContent |
 * -----------------------------------------------------------------------
 * The size of each part is:
 *  -----------------------------------------------------------------------
 * | 4             | 1       | 4          | #encodedLength | #bodyLength |
 * -----------------------------------------------------------------------
 * So the #headerLength is 4 + 1 + 4,
 * and the complete frameLength is:
 *   #frameLength = #headerLength + #encodedLength + #bodyLength.
 */

class MessageDecoder : public wangle::ByteToByteDecoder {
 public:
  MessageDecoder(bool networkByteOrder = true)
      : networkByteOrder_(networkByteOrder) {}

  bool decode(
      Context* ctx,
      folly::IOBufQueue& buf,
      std::unique_ptr<folly::IOBuf>& result,
      size_t&) override {
    if (buf.chainLength() < headerLength_) {
      return false;
    }

    folly::io::Cursor c(buf.front());
    int encodedLength, bodyLength;
    if (networkByteOrder_) {
      encodedLength = c.readBE<int32_t>();
      c.skip(1);
      bodyLength = c.readBE<int32_t>();
    } else {
      encodedLength = c.readLE<int32_t>();
      c.skip(1);
      bodyLength = c.readLE<int32_t>();
    }

    uint64_t frameLength = headerLength_ + encodedLength + bodyLength;
    if (buf.chainLength() < frameLength) {
      return false;
    }

    result = buf.split(frameLength);
    return true;
  }

 private:
  bool networkByteOrder_;

  static constexpr int lenEncodedLength_ = 4;
  static constexpr int lenMsgType_ = 1;
  static constexpr int lenBodyLength_ = 4;
  static constexpr int headerLength_ =
      lenEncodedLength_ + lenMsgType_ + lenBodyLength_;
};
} // namespace celeborn