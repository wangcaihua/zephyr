#ifndef ZEPHYR_GRPC_MANAGER_H_
#define ZEPHYR_GRPC_MANAGER_H_

#include "grpcpp/grpcpp.h"
#include "zephyr/common/rpc_manager.h"

namespace zephyr {
namespace grpc {

using ::grpc::Channel;
using ::grpc::ChannelArguments;
using ::grpc::ClientContext;
using zephyr::common::DoneCallBack;
using zephyr::common::RpcChannel;
using zephyr::common::RpcContext;
using zephyr::common::RpcManager;

class GrpcManager : public RpcManager {
public:
  GrpcManager() : RpcManager() {}

  unique_ptr<RpcChannel> CreateChannel(const string &host_port,
                                       int tag) override;

  RpcContext *CreateContext(const string &method, Message *response,
                            DoneCallBack done) override;
};

} // namespace grpc
} // namespace zephyr

#endif // ZEPHYR_GRPC_MANAGER_H_
