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

std::vector<std::string_view> parseColonSeparatedHostPorts(
    const std::string_view& s,
    int num) {
  auto parsed = explode(s, ':');
  CELEBORN_CHECK_GT(parsed.size(), num);
  std::vector<std::string_view> result;
  result.resize(num + 1);
  size_t size = 0;
  for (int result_idx = 1, parsed_idx = parsed.size() - num;
       result_idx <= num;
       result_idx++, parsed_idx++) {
    result[result_idx] = parsed[parsed_idx];
    size += parsed[parsed_idx].size() + 1;
  }
  result[0] = s.substr(0, s.size() - size);
  return result;
}

std::vector<std::string_view> explode(const std::string_view& s, char delim) {
  std::vector<std::string_view> result;
  std::string_view::size_type i = 0;
  while (i < s.size()) {
    auto j = s.find(delim, i);
    if (j == std::string::npos) {
      j = s.size();
    }
    result.emplace_back(s.substr(i, j - i));
    i = j + 1;
  }
  return result;
}

std::tuple<std::string_view, std::string_view> split(
    const std::string_view& s,
    char delim) {
  auto pos = s.find(delim);
  return std::make_tuple(s.substr(0, pos), s.substr(pos + 1));
}
} // namespace celeborn
