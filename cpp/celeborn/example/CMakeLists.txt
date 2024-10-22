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
        reader
        ${WANGLE}
        ${FIZZ}
        ${LIBSODIUM_LIBRARY}
        ${FOLLY_WITH_DEPENDENCIES}
        ${GLOG}
        ${GFLAGS_LIBRARIES}
)

add_executable(client ReaderClient.cpp)

target_link_libraries(client readerClient)