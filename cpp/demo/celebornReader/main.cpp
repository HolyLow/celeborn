#include <folly/init/Init.h>

#include <celeborn/network/Message.h>
#include <celeborn/network/TransportClient.h>
#include <celeborn/client/reader/CelebornInputStream.h>
#include <celeborn/client/ShuffleClient.h>

using namespace celeborn;

int lifecycleManagerPort;
std::string lifecycleManagerHost;
std::string localHost;
std::string lifecycleManagerName = "LifecycleManagerEndpoint";

void testFollyFuture() {
  std::cout << "main thread id " << std::this_thread::get_id() << std::endl;
  folly::Promise<int> promise;
  promise.setInterruptHandler([](const folly::exception_wrapper& e) {
    std::cout << "promiseInterrupt thread id " << std::this_thread::get_id()
              << std::endl;
    std::cout << "error msg: " << e.what() << std::endl;
  });
  auto future = promise.getFuture();
  auto t = std::thread([&promise]() {
    std::cout << "begin born thread id " << std::this_thread::get_id()
              << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(5));
    promise.setValue(1);
    std::cout << "finish born thread id " << std::this_thread::get_id()
              << std::endl;
  });

  // auto status = std::move(future).get(std::chrono::milliseconds(100));
  // std::cout << "status = " << status << std::endl;
  std::move(future)
      .thenValue([](int status) {
        std::cout << "thenValue thread id " << std::this_thread::get_id()
                  << std::endl;
        std::cout << "status = " << status << std::endl;
      })
      .onTimeout(std::chrono::milliseconds(100), []() {
        std::cout << "onTimeout thread id " << std::this_thread::get_id()
                  << std::endl;
      });
  t.join();
}

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  testFollyFuture();

  google::SetCommandLineOption("GLOG_minloglevel", "0");
  FLAGS_logtostderr = 1;

  std::ifstream inFile;
  inFile.open("/tmp/LifecycleManagerHost.txt");
  std::string ip, port;
  std::getline(inFile, ip, ':');
  std::getline(inFile, port);
  inFile.close();
  inFile.open("/tmp/shuffleKey.txt");
  std::string shuffleKey, appId, shuffleIdStr;
  std::getline(inFile, shuffleKey);
  std::size_t found = shuffleKey.find_last_of("-");
  appId = shuffleKey.substr(0, found);
  shuffleIdStr = shuffleKey.substr(found + 1);
  inFile.close();
  lifecycleManagerHost = ip;
  localHost = ip;
  lifecycleManagerPort = std::atoi(port.c_str());
  int shuffleId = std::atoi(shuffleIdStr.c_str());
  auto conf = std::make_shared<CelebornConf>();

  auto clientFactory = std::make_shared<TransportClientFactory>(conf);

  auto shuffleClient = std::make_unique<ShuffleClientImpl>(
      appId, conf, clientFactory);
  shuffleClient->setupLifecycleManagerRef(lifecycleManagerHost, lifecycleManagerPort);
  shuffleClient->updateReducerFileGroup(shuffleId);

  std::vector<std::unique_ptr<CelebornInputStream>> streams;
  for (int partitionId = 0; partitionId < 20; partitionId++) {
    auto inputStream =
        shuffleClient->readPartition(shuffleId, partitionId, 0, 0, INT_MAX);
    char buffer[4096];
    int readCnt = 0;
    int totalCnt = 0;
    do {
      readCnt = inputStream->read((uint8_t*)buffer, 0, 4096);
      if (readCnt > 0)
        totalCnt += readCnt;
    } while (readCnt > 0);

    streams.emplace_back(std::move(inputStream));
  }
}
