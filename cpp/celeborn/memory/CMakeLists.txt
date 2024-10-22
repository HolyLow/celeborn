add_library(
        memory
        ByteBuffer.cpp)


target_link_libraries(
        memory
        ${FOLLY_WITH_DEPENDENCIES}
        ${GLOG}
        ${GFLAGS_LIBRARIES}
)