# Minimum CMake required
cmake_minimum_required(VERSION 3.5)

project(ZephyrClient CXX)

enable_testing()

# Set C++11 as standard for the whole project
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0 -O3 -fno-omit-frame-pointer)

find_package(Threads)

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

# Add external module
include(grpc)
include(googletest)
include(glog)
include(zookeeper)

# Add include directories
include_directories(${GTEST_INCLUDE_DIRS}
        ${GLOG_INCLUDE_DIRS}
        ${CMAKE_CURRENT_BINARY_DIR}
        .)
include_directories(SYSTEM
        ${PROTOBUF_INCLUDE_DIRS}
        ${GRPC_INCLUDE_DIRS}
        ${ZOOKEEPER_INCLUDE_DIRS})

file(GLOB_RECURSE sources zephyr/zk/*.cpp zephyr/zh/*.h zephyr/client/*.cpp zephyr/client/*.h
        zephyr/grpc/*.h zephyr/grpc/*.cpp zephyr/kernels/*.h zephyr/kernels/*.cpp
        zephyr/ops/*.h zephyr/ops/*.cpp zephyr/common/*.h zephyr/common/*.cpp)


add_library(ZephyrClient SHARED ${sources})

# just for example add some compiler flags
target_compile_options(ZephyrClient PUBLIC -std=c++1y -Wall -Wfloat-conversion -D_GLIBCXX_USE_CXX11_ABI=0)

# this lets me include files relative to the root src dir with a <> pair
target_include_directories(ZephyrClient PUBLIC /zephyr)


