add_library(
        network
        TransportMessage.cpp
        Message.cpp
        TransportClient.cpp
        NettyRpcEndpointRef.cpp)

target_include_directories(network PUBLIC ${CMAKE_BINARY_DIR})

target_link_libraries(
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