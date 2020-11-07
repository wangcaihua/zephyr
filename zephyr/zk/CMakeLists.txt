add_definitions(-DTHREADED) # Enable Zookeeper Sync API.

file(GLOB_RECURSE zk_files *.cpp *.h)

add_library(zk STATIC ${zk_files})
target_link_libraries(zk zookeeper glog common)
