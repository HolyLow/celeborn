#include "protocol/PartitionLocation.h"

#include "celeborn/protocol/StatusCode.h"
#include "celeborn/utils/Exceptions.h"

namespace celeborn {
std::unique_ptr<StorageInfo> StorageInfo::fromPb(const PbStorageInfo& pb) {
  auto result = std::make_unique<StorageInfo>();
  result->type = (Type)pb.type();
  result->mountPoint = pb.mountpoint();
  result->finalResult = pb.finalresult();
  result->filePath = pb.filepath();
  result->availableStorageTypes = pb.availablestoragetypes();
  return std::move(result);
}

std::unique_ptr<const PartitionLocation> PartitionLocation::fromPb(
    const PbPartitionLocation& pb) {
  auto result = fromPbWithoutPeer(pb);
  if (pb.has_peer()) {
    auto peer = fromPbWithoutPeer(pb.peer());
    if (result->mode == PRIMARY) {
      CELEBORN_CHECK(
          peer->mode == REPLICA, "PRIMARY's peer mode should be REPLICA");
      result->replicaPeer = std::move(peer);
    } else {
      CELEBORN_CHECK(
          peer->mode == PRIMARY, "REPLICA's peer mode should be PRIMARY");
      peer->replicaPeer = std::move(result);
      result = std::move(peer);
    }
  }
  CELEBORN_CHECK(result->mode == PRIMARY, "non-peer's mode should be PRIMARY");
  return std::move(result);
}

PartitionLocation::PartitionLocation(const PartitionLocation& other)
    : id(other.id),
      epoch(other.epoch),
      host(other.host),
      rpcPort(other.rpcPort),
      pushPort(other.pushPort),
      fetchPort(other.fetchPort),
      replicatePort(other.replicatePort),
      mode(other.mode),
      replicaPeer(
          other.replicaPeer
              ? std::make_unique<PartitionLocation>(*other.replicaPeer)
              : nullptr),
      storageInfo(std::make_unique<StorageInfo>(*other.storageInfo)) {}

std::unique_ptr<PartitionLocation> PartitionLocation::fromPbWithoutPeer(
    const PbPartitionLocation& pb) {
  auto result = std::make_unique<PartitionLocation>();
  result->id = pb.id();
  result->epoch = pb.epoch();
  result->host = pb.host();
  result->rpcPort = pb.rpcport();
  result->pushPort = pb.pushport();
  result->fetchPort = pb.fetchport();
  result->replicatePort = pb.replicateport();
  result->mode = static_cast<Mode>(pb.mode());
  result->replicaPeer = nullptr;
  result->storageInfo = StorageInfo::fromPb(pb.storageinfo());
  return std::move(result);
}

StatusCode toStatusCode(int32_t code) {
  CELEBORN_CHECK(code >= 0);
  CELEBORN_CHECK(code <= StatusCode::TAIL);
  return static_cast<StatusCode>(code);
}
} // namespace celeborn
