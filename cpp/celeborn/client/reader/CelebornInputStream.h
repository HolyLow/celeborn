#pragma once

#include "celeborn/conf/CelebornConf.h"
#include "celeborn/client/reader/WorkerPartitionReader.h"

namespace celeborn {
class CelebornInputStream {
 public:
  CelebornInputStream(
      const std::string& shuffleKey,
      const std::shared_ptr<const CelebornConf>& conf,
      const std::shared_ptr<TransportClientFactory>& clientFactory,
      std::vector<std::shared_ptr<const PartitionLocation>>&& locations,
      const std::vector<int>& attempts,
      int attemptNumber,
      int startMapIndex,
      int endMapIndex);

  int read(uint8_t* buffer, size_t offset, size_t len);

 private:
  bool fillBuffer();

  bool moveToNextChunk();

  std::unique_ptr<ReadOnlyByteBuffer> getNextChunk();

  void verifyChunk(const std::unique_ptr<ReadOnlyByteBuffer>& chunk);

  void moveToNextReader();

  std::shared_ptr<PartitionReader> createReaderWithRetry(
      const PartitionLocation& location);

  std::shared_ptr<PartitionReader> createReader(
      const PartitionLocation& location);

  std::shared_ptr<const PartitionLocation> nextReadableLocation();

  std::unordered_set<int>& getBatchRecord(int mapId);

  void cleanupReader();

  std::string shuffleKey_;
  std::shared_ptr<const CelebornConf> conf_;
  std::shared_ptr<TransportClientFactory> clientFactory_;
  std::vector<std::shared_ptr<const PartitionLocation>> locations_;
  std::vector<int> attempts_;
  int attemptNumber_;
  int startMapIndex_;
  int endMapIndex_;

  int currLocationIndex_;
  std::unique_ptr<ReadOnlyByteBuffer> currChunk_;
  size_t currBatchPos_;
  size_t currBatchSize_;
  std::shared_ptr<PartitionReader> currReader_;
  std::vector<std::unique_ptr<std::unordered_set<int>>> batchRecords_;
  std::vector<std::shared_ptr<PartitionReader>> readers_;
};
} // namespace celeborn