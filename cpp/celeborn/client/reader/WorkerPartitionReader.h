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

#include "celeborn/network/TransportClient.h"
#include "celeborn/protocol/PartitionLocation.h"

namespace celeborn {
class PartitionReader {
 public:
  virtual ~PartitionReader() = default;

  virtual bool hasNext() = 0;

  virtual std::unique_ptr<ReadOnlyByteBuffer> next() = 0;
};

class WorkerPartitionReader
    : public PartitionReader,
      public std::enable_shared_from_this<WorkerPartitionReader> {
 public:
  // Only allow using create method to get the shared_ptr holder. This is
  // required by the std::enable_shared_from_this functionality.
  static std::shared_ptr<WorkerPartitionReader> create(
      const std::shared_ptr<const CelebornConf>& conf,
      const std::string& shuffleKey,
      const PartitionLocation& location,
      int32_t startMapIndex,
      int32_t endMapIndex,
      TransportClientFactory& clientFactory);

  ~WorkerPartitionReader() override;

  bool hasNext() override;

  std::unique_ptr<ReadOnlyByteBuffer> next() override;

  void fetchChunks();

 private:
  // Disable creating the object directly to make sure that
  // std::enable_shared_from_this works properly.
  WorkerPartitionReader(
      const std::shared_ptr<const CelebornConf>& conf,
      const std::string& shuffleKey,
      const PartitionLocation& location,
      int32_t startMapIndex,
      int32_t endMapIndex,
      TransportClientFactory& clientFactory);

  // This function cannot be called within constructor!
  void initAndCheck();

  std::string shuffleKey_;
  PartitionLocation location_;
  std::shared_ptr<TransportClient> client_;
  int32_t startMapIndex_;
  int32_t endMapIndex_;
  std::unique_ptr<StreamHandler> streamHandler_;

  int32_t fetchingChunkId_;
  int32_t toConsumeChunkId_;
  int32_t maxFetchChunksInFlight_;
  Timeout fetchTimeout_;

  folly::UMPSCQueue<std::unique_ptr<ReadOnlyByteBuffer>, true> chunkQueue_;
  FetchChunkSuccessCallback onSuccess_;
  FetchChunkFailureCallback onFailure_;
  folly::Synchronized<std::unique_ptr<std::exception>> exception_;

  static constexpr auto kDefaultConsumeIter = std::chrono::milliseconds(500);
  // TODO: the maxTry count might not be proper.
  static constexpr int kDefaultMaxTryConsume = 100;

  // TODO: add other params, such as fetchChunkRetryCnt, fetchChunkMaxRetry
};
} // namespace celeborn
