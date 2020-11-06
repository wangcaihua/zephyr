#ifndef ZEPHYR_GRPC_MANAGER_H_
#define ZEPHYR_GRPC_MANAGER_H_

#include <string>

#include "zephyr/grpc/rpc_manager.h"

using zephyr::common::Status;
using zephyr::common::StatusCode;

namespace zephyr {
namespace grpc {

class GrpcManager : public RpcManager {
 public:
  GrpcManager() : RpcManager() { }

  std::unique_ptr<RpcChannel> CreateChannel(
       const std::string &host_port, int tag) override;

  RpcContext *CreateContext(
      const std::string &method,
      google::protobuf::Message *response,
      std::function<void(const Status &)> done) override;
};

}  // namespace grpc
}  // namespace zephyr

#endif  // ZEPHYR_GRPC_MANAGER_H_
