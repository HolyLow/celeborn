#pragma once

#include <chrono>
#include <set>
#include <vector>

#include "celeborn/memory/ByteBuffer.h"

namespace celeborn {
#define CELEBORN_STARTUP_LOG_PREFIX "[CELEBORN_STARTUP] "
#define CELEBORN_STARTUP_LOG(severity) \
  LOG(severity) << CELEBORN_STARTUP_LOG_PREFIX

#define CELEBORN_SHUTDOWN_LOG_PREFIX "[CELEBORN_SHUTDOWN] "
#define CELEBORN_SHUTDOWN_LOG(severity) \
  LOG(severity) << CELEBORN_SHUTDOWN_LOG_PREFIX

template <typename T>
std::vector<T> toVector(const std::set<T>& in) {
  std::vector<T> out{};
  out.reserve(in.size());
  for (const auto& i : in) {
    out.emplace_back(i);
  }
  return std::move(out);
}

std::string makeShuffleKey(const std::string& appId, int shuffleId);

void writeUTF(folly::io::Appender& appender, std::string msg);

void writeRpcAddress(folly::io::Appender& appender, std::string host, int port);

void writeUTF(WriteOnlyByteBuffer& buffer, std::string& msg);

void writeRpcAddress(WriteOnlyByteBuffer& buffer, std::string& host, int port);

using Duration = std::chrono::duration<double>;
using Timeout = std::chrono::milliseconds;
inline Timeout toTimeout(Duration duration) {
  return std::chrono::duration_cast<Timeout>(duration);
}
} // namespace celeborn
