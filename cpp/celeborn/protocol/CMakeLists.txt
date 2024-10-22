add_library(
        protocol
        ControlMessages.cpp
        PartitionLocation.cpp)

target_include_directories(protocol PUBLIC ${CMAKE_BINARY_DIR})

target_link_libraries(
        protocol
        memory
        proto
        network
        ${FOLLY_WITH_DEPENDENCIES}
        ${GLOG}
        ${GFLAGS_LIBRARIES}
)