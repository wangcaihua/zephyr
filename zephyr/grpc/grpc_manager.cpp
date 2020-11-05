#include "zephyr/grpc/grpc_manager.h"

#include <string>

#include "zephyr/grpc/grpc_channel.h"

namespace zephyr {
namespace grpc {

std::unique_ptr<RpcChannel> GrpcManager::CreateChannel(
    const std::string &host_port, int tag) {
  grpc::ChannelArguments args;
  args.SetMaxReceiveMessageSize(-1);
  args.SetInt("tag", tag);
  std::shared_ptr<grpc::Channel> raw_channel =
      grpc::CreateCustomChannel(host_port,
                                grpc::InsecureChannelCredentials(),
                                args);
  return std::unique_ptr<RpcChannel>(new GrpcChannel(host_port, raw_channel));
}

RpcContext *GrpcManager::CreateContext(
    const std::string &method, google::protobuf::Message *respone,
    std::function<void(const Status &)> done) {
  return new GrpcContext(method, respone, done);
}

}  // namespace grpc
}  // namespace zephyr
