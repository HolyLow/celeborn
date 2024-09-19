#include "celeborn/utils/CelebornUtils.h"

namespace celeborn {
std::string makeShuffleKey(const std::string& appId, const int shuffleId) {
  return appId + "-" + std::to_string(shuffleId);
}

void writeUTF(folly::io::Appender& appender, std::string msg) {
  appender.writeBE<short>(msg.size());
  appender.push((const uint8_t*)msg.c_str(), msg.size());
}

void writeRpcAddress(
    folly::io::Appender& appender,
    std::string host,
    int port) {
  appender.write<uint8_t>(1);
  writeUTF(appender, host);
  appender.writeBE<int32_t>(port);
}

void writeUTF(WriteOnlyByteBuffer& buffer, std::string& msg) {
  buffer.write<short>(msg.size());
  buffer.writeFromString(msg);
}

void writeRpcAddress(WriteOnlyByteBuffer& buffer, std::string& host, int port) {
  buffer.write<uint8_t>(1);
  writeUTF(buffer, host);
  buffer.write<int32_t>(port);
}
} // namespace celeborn
