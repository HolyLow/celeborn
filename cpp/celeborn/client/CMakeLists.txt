add_library(
        client
        reader/CelebornInputStream.cpp
        ShuffleClient.cpp
        reader/WorkerPartitionReader.cpp)

target_include_directories(client PUBLIC ${CMAKE_BINARY_DIR})

target_link_libraries(
        client
        network
        proto
        memory
        protocol
        utils
        ${WANGLE}
        ${FIZZ}
        ${LIBSODIUM_LIBRARY}
        ${FOLLY_WITH_DEPENDENCIES}
        ${GLOG}
        ${GFLAGS_LIBRARIES}
)