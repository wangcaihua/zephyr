#ifndef ZEPHYR_RPC_CLIENT_H_
#define ZEPHYR_RPC_CLIENT_H_

#include <string>

#include "google/protobuf/message.h"

#include "zephyr/common/zephyr_config.h"
#include "zephyr/grpc/impl_register.h"
#include "zephyr/grpc/rpc_manager.h"
#include "zephyr/common/status.h"
#include "zephyr/zk/server_monitor.h"

using zephyr::common::Status;
using zephyr::common::StatusCode;
using zephyr::zk::ServerMonitor;
using zephyr::common::ZephyrConfig;

namespace zephyr {
namespace grpc {

class RpcClient {
 public:
  virtual bool Initialize(std::shared_ptr<ServerMonitor> monitor,
                          size_t shard_index, const ZephyrConfig &config) = 0;
  virtual void IssueRpcCall(const std::string &method,
                            const google::protobuf::Message &request,
                            google::protobuf::Message *response,
                            std::function<void(const Status &)> done) = 0;
  virtual ~RpcClient() = default;
};

class RpcClientBase : public RpcClient {
 public:
  RpcClientBase() : rpc_manager_(ImplFactory<RpcManager>::New()),
                    num_retries_(kRpcRetryCount) { }

  bool Initialize(std::shared_ptr<ServerMonitor> monitor,
                  size_t shard_index, const ZephyrConfig &config) override;
  void IssueRpcCall(const std::string &method,
                    const google::protobuf::Message &request,
                    google::protobuf::Message *respone,
                    std::function<void(const Status &)> done) override;

 private:
  static constexpr const int kRpcRetryCount = 10;

  void DoIssueRpcCall(RpcContext *ctx);

  std::unique_ptr<RpcManager> rpc_manager_;
  int num_retries_;
};

std::unique_ptr<RpcClient> NewRpcClient(
    std::shared_ptr<ServerMonitor> monitor, size_t shard_index,
    const ZephyrConfig &config = ZephyrConfig());

}  // namespace grpc
}  // namespace zephyr

#endif  // ZEPHYR_RPC_CLIENT_H_
