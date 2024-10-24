add_library(
        readerClient
        ReaderClient.cpp)

target_link_libraries(
        readerClient
        memory
        utils
        conf
        proto
        network
        protocol
        client
        ${WANGLE}
        ${FIZZ}
        ${LIBSODIUM_LIBRARY}
        ${FOLLY_WITH_DEPENDENCIES}
        ${GLOG}
        ${GFLAGS_LIBRARIES}
)

add_executable(reader ReaderClient.cpp)

target_link_libraries(reader readerClient)