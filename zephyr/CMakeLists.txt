if (BUILD_COVERAGE)
    set(CMAKE_CXX_FLAGS "-g -O0 --coverage")
    set (CMAKE_EXE_LINKER_FLAGS "--coverage")
endif(BUILD_COVERAGE)


# Add subdirectories
#add_subdirectory(client)
add_subdirectory(common)
add_subdirectory(ipc)
add_subdirectory(zk)
add_subdirectory(proto)


# add_library(ZephyrClient SHARED ${sources})

# just for example add some compiler flags
# target_compile_options(ZephyrClient PUBLIC -std=c++1y -Wall -Wfloat-conversion -D_GLIBCXX_USE_CXX11_ABI=0)

# this lets me include files relative to the root src dir with a <> pair
# target_include_directories(ZephyrClient PUBLIC /zephyr)

# target_link_libraries(proto glog zookeeper jemalloc_STATIC_PIC)