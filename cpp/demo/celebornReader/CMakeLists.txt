cmake_minimum_required(VERSION 3.24)
project(celebornReader)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

if("$ENV{THIRDPARTY_DIR}" STREQUAL "")
    set(THIRDPARTY_DIR ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty)
else()
    set(THIRDPARTY_DIR $ENV{THIRDPARTY_DIR})
endif()
set(THIRDPARTY_INSTALL_DIR ${THIRDPARTY_DIR}/installed)
# Use "PREPEND" to look in thirdparty prefix paths before anywhere else for
# system dependencies.
list(PREPEND CMAKE_PREFIX_PATH ${THIRDPARTY_INSTALL_DIR})
message(STATUS "Using CMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH}")
# Currently, we use the "-lgtest" to link the gtest library, and gtest library
# is in the "installed/lib64" directory in the linux environment, so the lib
# search path needs to be specified here.
if(EXISTS ${THIRDPARTY_INSTALL_DIR}/lib64)
    link_directories(${THIRDPARTY_INSTALL_DIR}/lib64)
    # thrift.a is installed in the directory  ${THIRDPARTY_INSTALL_DIR}/lib by
    # default in the Linux environment.
    link_directories(${THIRDPARTY_INSTALL_DIR}/lib)
elseif(CMAKE_SYSTEM_NAME MATCHES "Linux")
    message(
            WARNING
            "You did not use the recommended way "
            "(using 'thirdparty/build-thirdparty.sh') to build & install thirdparty "
            "libraries in '${THIRDPARTY_INSTALL_DIR}'.")
endif()

#if (NOT APPLE)
#    set(Boost_USE_STATIC_RUNTIME ON)
#endif ()
set(Boost_USE_MULTITHREADED TRUE)
find_package(
        Boost
        1.75.0
        REQUIRED
        program_options
        context
        filesystem
        regex
        thread
        system
        date_time
        atomic)
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})

if (APPLE)
    # Use the shared library of gflags on MacOS because it is installed via
    # Homebrew and only shared library is installed.
    find_package(gflags REQUIRED COMPONENTS shared)
else ()
    find_package(gflags REQUIRED COMPONENTS static)
endif ()

# Try to avoid folly::f14::detail::F14LinkCheck problem
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2")

find_library(FIZZ fizz REQUIRED)
find_library(CELEBORN celeborn)

set(CMAKE_MODULE_PATH
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/"
        #        # for in-fbsource builds
        #        "${CMAKE_CURRENT_SOURCE_DIR}/../opensource/fbcode_builder/CMake"
        #        # For shipit-transformed builds
        #        "${CMAKE_CURRENT_SOURCE_DIR}/../build/fbcode_builder/CMake"
        ${CMAKE_MODULE_PATH})
find_package(folly CONFIG REQUIRED)
find_package(Sodium REQUIRED)
find_package(fizz CONFIG REQUIRED)
find_package(wangle CONFIG REQUIRED)
find_library(RE2 re2)
find_package(celeborn REQUIRED)

add_executable(celebornReader main.cpp)

target_include_directories(celebornReader PRIVATE ${FOLLY_INCLUDE_DIR} ${CELEBORN_INCLUDE_DIR}
        ${GFLAGS_INCLUDE_DIRS})

target_link_libraries(celebornReader ${FOLLY_LIBRARIES} ${CELEBORN_LIBRARIES}
        ${GFLAGS_LIBRARIES}
        ${RE2})
