/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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