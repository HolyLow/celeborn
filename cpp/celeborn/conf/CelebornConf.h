#pragma once

#include "celeborn/conf/BaseConf.h"
#include "celeborn/utils/CelebornUtils.h"

namespace celeborn {
/***
 * steps to add a new config:
 * === in CelebornConf.h:
 *     1. define the configName with "static constexpr std::string_view";
 *     2. declare the getter method within class;
 * === in CelebornConf.cpp:
 *     3. register the configName in CelebornConf's constructor, with proper
 *        data type and proper default value;
 *     4. implement the getter method.
 */

class CelebornConf : public BaseConf {
public:
  static constexpr std::string_view kRpcLookupTimeout{
    "celeborn.rpc.lookupTimeout"};

  static constexpr std::string_view kClientRpcGetReducerFileGroupRpcAskTimeout{
      "celeborn.client.rpc.getReducerFileGroup.askTimeout"};

  static constexpr std::string_view kNetworkConnectTimeout{
      "celeborn.network.connect.timeout"};

  static constexpr std::string_view kClientFetchTimeout{
      "celeborn.client.fetch.timeout"};

  static constexpr std::string_view kNetworkIoNumConnectionsPerPeer{
      "celeborn.data.io.numConnectionsPerPeer"};

  static constexpr std::string_view kNetworkIoClientThreads{
      "celeborn.data.io.clientThreads"};

  static constexpr std::string_view kClientFetchMaxReqsInFlight{
      "celeborn.client.fetch.maxReqsInFlight"};

  CelebornConf();

  Timeout rpcLookupTimeout() const;

  Timeout clientRpcGetReducerFileGroupRpcAskTimeout() const;

  Timeout networkConnectTimeout() const;

  Timeout clientFetchTimeout() const;

  int networkIoNumConnectionsPerPeer() const;

  int networkIoClientThreads() const;

  int clientFetchMaxReqsInFlight() const;
};
} // namespace celeborn