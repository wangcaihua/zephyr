#include "zephyr/ipc/grpc_manager.h"
#include "zephyr/common/impl_register.h"
#include "zephyr/ipc/grpc_channel.h"

namespace zephyr {
namespace grpc {

using zephyr::common::ImplFactory;

unique_ptr<RpcChannel> GrpcManager::CreateChannel(const string &host_port,
                                                  int tag) {
  ChannelArguments args;
  args.SetMaxReceiveMessageSize(-1);
  args.SetInt("tag", tag);
  shared_ptr<Channel> raw_channel = ::grpc::CreateCustomChannel(
      host_port, ::grpc::InsecureChannelCredentials(), args);
  return unique_ptr<RpcChannel>(new GrpcChannel(host_port, raw_channel));
}

RpcContext *GrpcManager::CreateContext(const string &method, Message *response,
                                       DoneCallBack done) {
  return new GrpcContext(method, response, done);
}

REGISTER_IMPL(RpcManager, GrpcManager);

} // namespace grpc
} // namespace zephyr
