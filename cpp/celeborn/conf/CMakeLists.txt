add_library(
        conf
        BaseConf.cpp
        CelebornConf.cpp)

target_link_libraries(
        conf
        utils
        memory
        ${FOLLY_WITH_DEPENDENCIES}
        ${GLOG}
        ${GFLAGS_LIBRARIES}
        ${RE2}
)