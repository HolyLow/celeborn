#pragma once

#include <map>
#include <set>

#include "celeborn/network/TransportMessage.h"
#include "celeborn/protocol/PartitionLocation.h"
#include "celeborn/protocol/StatusCode.h"

namespace celeborn {
struct GetReducerFileGroup {
  int shuffleId;

  TransportMessage toTransportMessage() const;
};

struct GetReducerFileGroupResponse {
  StatusCode status;
  std::map<int, std::set<std::shared_ptr<const PartitionLocation>>> fileGroups;
  std::vector<int> attempts;
  std::set<int> partitionIds;

  static std::unique_ptr<GetReducerFileGroupResponse> fromTransportMessage(
      const TransportMessage& transportMessage);
};

struct OpenStream {
  std::string shuffleKey;
  std::string filename;
  int32_t startMapIndex;
  int32_t endMapIndex;

  OpenStream(
      const std::string& shuffleKey,
      const std::string& filename,
      int32_t startMapIndex,
      int32_t endMapIndex);

  TransportMessage toTransportMessage() const;
};

struct StreamHandler {
  int64_t streamId;
  int32_t numChunks;
  std::vector<int64_t> chunkOffsets;
  std::string fullPath;

  static std::unique_ptr<StreamHandler> fromTransportMessage(
      const TransportMessage& transportMessage);
};

struct StreamChunkSlice {
  long streamId;
  int chunkIndex;
  int offset{0};
  int len{INT_MAX};

  std::unique_ptr<PbStreamChunkSlice> toProto() const;

  static StreamChunkSlice decodeFrom(ReadOnlyByteBuffer& data);

  std::string toString() const {
    return std::to_string(streamId) + "-" + std::to_string(chunkIndex) + "-" +
        std::to_string(offset) + "-" + std::to_string(len);
  }

  bool operator==(const StreamChunkSlice& rhs) const {
    return streamId == rhs.streamId && chunkIndex == rhs.chunkIndex &&
        offset == rhs.offset && len == rhs.len;
  }

  struct Hasher {
    size_t operator()(const StreamChunkSlice& lhs) const;
  };
};

struct ChunkFetchRequest {
  StreamChunkSlice streamChunkSlice;

  TransportMessage toTransportMessage() const;
};

struct BufferStreamEnd {
  long streamId;

  TransportMessage toTransportMessage() const;
};
} // namespace celeborn
