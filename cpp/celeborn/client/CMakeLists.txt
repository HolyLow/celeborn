add_library(
        reader
        reader/CelebornInputStream.cpp
        ShuffleClient.cpp
        reader/WorkerPartitionReader.cpp)

target_include_directories(reader PUBLIC ${CMAKE_BINARY_DIR})

target_link_libraries(
        reader
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