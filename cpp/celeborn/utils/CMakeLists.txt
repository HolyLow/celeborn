add_library(
        utils
        CelebornException.cpp ProcessBase.cpp StackTrace.cpp Exceptions.cpp flags.cpp CelebornUtils.cpp)

target_link_libraries(
        utils
        memory
        ${WANGLE}
        ${FIZZ}
        ${LIBSODIUM_LIBRARY}
        ${FOLLY_WITH_DEPENDENCIES}
        ${GLOG}
        ${GFLAGS_LIBRARIES}
)