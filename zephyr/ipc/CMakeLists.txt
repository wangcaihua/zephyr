file(GLOB_RECURSE grpc_files *.cpp *.h)

add_library(ipc STATIC ${grpc_files})
target_link_libraries(ipc common glog jemalloc_STATIC_PIC)