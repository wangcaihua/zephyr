protobuf_generate_grpc_cpp(zephyr.proto)

#add_library(proto zephyr.pb.* zephyr.grpc.pb.*)
#target_link_libraries(proto libprotobuf grpc++_unsecure)
#target_compile_options(proto PRIVATE -Wno-unused-parameter)
