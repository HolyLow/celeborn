# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
cmake_minimum_required(VERSION 3.24)

if (NOT DEFINED PACKAGE_VERSION)
    set(PACKAGE_VERSION "0.0.1")
endif ()

project("celeborn" VERSION ${PACKAGE_VERSION} LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
message("Appending CMAKE_CXX_FLAGS with ${SCRIPT_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SCRIPT_CXX_FLAGS}")
if ("${TREAT_WARNINGS_AS_ERRORS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
endif ()

# Avoid folly::f14::detail::F14LinkCheck problem on x86-64 platform.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2")

# Set CMAKE_BUILD_TYPE to 'Release' if it is not specified.
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif ()
message(STATUS "CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}")

# Known warnings that are benign can be disabled.
set(DISABLED_WARNINGS
        "-Wno-nullability-completeness -Wno-deprecated-declarations")

# Important warnings that must be explicitly enabled.
set(ENABLE_WARNINGS "-Wreorder")

# The CMAKE_PREFIX_PATH should be set to the thirdparty's install path
# (thirdparty/installed by default), to find all the dependencies.
message(STATUS "Using CMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH}")
# Currently, we use the "-lgtest" to link the gtest library, and gtest library
# is in the "thirdparty/installed/lib64" directory in the linux environment, so
# the lib search path needs to be specified here.
if (EXISTS ${CMAKE_PREFIX_PATH}/lib64)
    link_directories(${CMAKE_PREFIX_PATH}/lib64)
    # thrift.a is installed in the directory thirdparty/installed/lib by
    # default in the Linux environment.
    link_directories(${CMAKE_PREFIX_PATH}/lib)
elseif (CMAKE_SYSTEM_NAME MATCHES "Linux")
    message(
            WARNING
            "You did not use the recommended way "
            "(using 'thirdparty/build-thirdparty.sh') to build & install "
            "thirdparty libraries.")
endif ()

#if (NOT APPLE)
#    set(Boost_USE_STATIC_RUNTIME ON)
#    set(Boost_USE_STATIC_LIBS ON)
#endif ()
set(Boost_USE_MULTITHREADED TRUE)
find_package(
        Boost
	#1.75.0
        1.84.0
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

# GFlags
#
# NOTE: The name here must be exactly "gflags", that is, use all lowercase.
# Otherwise, e.g. "GFLAGS" or "GFlags", the generated `GFLAGS_LIBRARIES` will
# point to the shared library instead of the static library, even if we
# explicitly specify to link against the static library (via "COMPONENTS
# static"). This may be a problem that the cmake script of GFlags does not
# consider comprehensively (for the case of words).
#
# See [1] for the recommended `find_package` commands to use to find GFlags, in
# which the "@PACKAGE_NAME@" will be replaced with "gflags" when installed.
#
# [1] https://github.com/gflags/gflags/blob/v2.2.2/cmake/config.cmake.in#L50-L56
if (APPLE)
    # Use the shared library of gflags on MacOS because it is installed via
    # Homebrew and only shared library is installed.
    find_package(gflags REQUIRED COMPONENTS shared)
else ()
    find_package(gflags REQUIRED COMPONENTS static)
endif ()

find_package(glog REQUIRED)
find_library(FMT fmt)

find_package(folly CONFIG REQUIRED)
set(FOLLY_WITH_DEPENDENCIES
        ${FOLLY_LIBRARIES}
        Boost::context
        dl
)

# Include third party header files
find_path(OPT_OPENSSL_DIR NAMES opt/openssl@1.1)
set(OPENSSL_ROOT_DIR "${OPT_OPENSSL_DIR}/opt/openssl@1.1")
find_package(OpenSSL REQUIRED)

find_package(Protobuf REQUIRED)

set(CMAKE_MODULE_PATH
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake"
        ${CMAKE_MODULE_PATH})
find_package(Sodium REQUIRED)
find_library(FIZZ fizz REQUIRED)
find_library(WANGLE wangle REQUIRED)

find_library(RE2 re2)

find_package(fizz CONFIG REQUIRED)
find_package(wangle CONFIG REQUIRED)

set(WANGLE_LIBRARIES ${WANGLE} ${FIZZ})

include_directories(SYSTEM ${OPENSSL_INCLUDE_DIR})

include_directories(SYSTEM celeborn)
include_directories(.)

### the following install scripts are based on wangle's cmakelist

# When installing Folly & Wangle in a non-default prefix, this will let
# projects linking against libwangle.so to find libfolly.so automatically.
SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(INCLUDE_INSTALL_DIR include CACHE STRING
        "The subdirectory where header files should be installed")
set(LIB_INSTALL_DIR lib CACHE STRING
        "The subdirectory where libraries should be installed")
set(CMAKE_INSTALL_DIR lib/cmake/celeborn CACHE STRING
        "The subdirectory where CMake package config files should be installed")

## TODO: to avoid the file location dependency problem caused by protobuf_cpp_generate,
## we hardcode the protoc procedure here. Maybe we could find a more elegant way to fix
## this later...
set(ProtoFile "${CMAKE_CURRENT_SOURCE_DIR}/celeborn/proto/TransportMessagesCpp.proto")
set(ProtoGenHeader "${CMAKE_BINARY_DIR}/celeborn/proto/TransportMessagesCpp.pb.h")
set(ProtoGenSource "${CMAKE_BINARY_DIR}/celeborn/proto/TransportMessagesCpp.pb.cc")
add_custom_command(
        OUTPUT ${ProtoGenSource} ${ProtoGenHeader}
        COMMAND mkdir -p ${CMAKE_BINARY_DIR}/celeborn/proto
        COMMAND protoc --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/celeborn/proto --cpp_out=${CMAKE_BINARY_DIR}/celeborn/proto ${ProtoFile}
        DEPENDS ${ProtoFile}
        VERBATIM)

set(CELEBORN_SHALLOW_HEADER_DIRS
        celeborn/conf
        celeborn/memory
        celeborn/network
        celeborn/protocol
        celeborn/client
        celeborn/utils
)

set(CELEBORN_CLIENT_DEEP_HEADER_DIRS celeborn/client/reader)

set(CELEBORN_HEADER_DIRS
        ${CELEBORN_SHALLOW_HEADER_DIRS}
        ${CELEBORN_CLIENT_DEEP_HEADER_DIRS}
)

foreach (dir ${CELEBORN_HEADER_DIRS})
    file(GLOB_RECURSE headers ${dir}/*.h)
    set(CELEBORN_HEADERS
            ${CELEBORN_HEADERS}
            ${headers})
endforeach ()

set(CELEBORN_SOURCE_DIRS ${CELEBORN_HEADER_DIRS})

foreach (dir ${CELEBORN_SOURCE_DIRS})
    file(GLOB_RECURSE sources ${dir}/*.cpp)
    set(CELEBORN_SOURCES
            ${CELEBORN_SOURCES}
            ${sources})
endforeach ()

include_directories(${CMAKE_BINARY_DIR})
add_library(celeborn ${CELEBORN_HEADERS} ${CELEBORN_SOURCES} ${ProtoGenSource} ${ProtoGenHeader})

target_include_directories(
        celeborn
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/..>
        $<INSTALL_INTERFACE:${INCLUDE_INSTALL_DIR}>
        ${FIZZ_INCLUDE_DIR}
        ${FOLLY_INCLUDE_DIR}
        ${Boost_INCLUDE_DIRS}
        ${OPENSSL_INCLUDE_DIR}
        ${GLOG_INCLUDE_DIRS}
        ${GFLAGS_INCLUDE_DIRS}
        ${PROTOBUF_INCLUDE_LIBRARY}
        ${WANGLE_INCLUDE_DIR}
)

target_link_libraries(
        celeborn
        PUBLIC
        ${FOLLY_LIBRARIES}
        ${FIZZ_LIBRARIES}
        ${Boost_LIBRARIES}
        ${OPENSSL_LIBRARIES}
        ${GLOG_LIBRARIES}
        ${GFLAGS_LIBRARIES}
        ${CMAKE_DL_LIBS}
        ${LIBRT_LIBRARIES}
        ${PROTOBUF_LIBRARY}
        ${WANGLE}
        Threads::Threads
)

install(
        TARGETS celeborn
        EXPORT celeborn-exports
        DESTINATION ${LIB_INSTALL_DIR}
)
# We unfortunately cannot install celeborn's headers with the install()
# statement above.  install(TARGETS) appears to only support installing
# PUBLIC_HEADER in a flat include directory, and not a deeper tree.
foreach (dir ${CELEBORN_SHALLOW_HEADER_DIRS})
    install(DIRECTORY ${dir} DESTINATION "${INCLUDE_INSTALL_DIR}/celeborn"
            FILES_MATCHING PATTERN "*.h")
endforeach ()
foreach (dir ${CELEBORN_CLIENT_DEEP_HEADER_DIRS})
    install(DIRECTORY ${dir} DESTINATION "${INCLUDE_INSTALL_DIR}/celeborn/client"
            FILES_MATCHING PATTERN "*.h")
endforeach ()
install(FILES ${ProtoGenHeader} DESTINATION "${INCLUDE_INSTALL_DIR}/celeborn/proto")

# Install CMake package configuration files for celeborn
include(CMakePackageConfigHelpers)
configure_package_config_file(
        cmake/celeborn-config.cmake.in
        celeborn-config.cmake
        INSTALL_DESTINATION ${CMAKE_INSTALL_DIR}
        PATH_VARS
        INCLUDE_INSTALL_DIR
        CMAKE_INSTALL_DIR
)
install(
        FILES ${CMAKE_CURRENT_BINARY_DIR}/celeborn-config.cmake
        DESTINATION ${CMAKE_INSTALL_DIR}
)
install(EXPORT celeborn-exports
        FILE celeborn-targets.cmake
        NAMESPACE celeborn::
        DESTINATION ${CMAKE_INSTALL_DIR})

option(BUILD_EXAMPLES "BUILD_EXAMPLES" ON)

if(BUILD_EXAMPLES)
    add_subdirectory(celeborn)
endif()