#ifndef ZEPHYR_RPC_CLIENT_H_
#define ZEPHYR_RPC_CLIENT_H_

#include "google/protobuf/message.h"
#include "impl_register.h"
#include "rpc_manager.h"
#include "server_monitor.h"
#include "status.h"
#include "zephyr/utils/imports.h"
#include "zephyr_config.h"

namespace zephyr {
namespace common {

class RpcClient {
public:
  virtual bool Initialize(shared_ptr<ServerMonitor> monitor, size_t shard_index,
                          const ZephyrConfig &config) = 0;
  virtual void IssueRpcCall(const string &method, const Message &request,
                            Message *response, DoneCallBack done) = 0;
  virtual ~RpcClient() = default;
};

class RpcClientBase : public RpcClient {
public:
  RpcClientBase()
      : rpc_manager_(ImplFactory<RpcManager>::New()),
        num_retries_(kRpcRetryCount) {}

  bool Initialize(shared_ptr<ServerMonitor> monitor, size_t shard_index,
                  const ZephyrConfig &config) override;

  void IssueRpcCall(const string &method, const Message &request,
                    Message *response, DoneCallBack done) override;

private:
  static constexpr const int kRpcRetryCount = 10;

  void DoIssueRpcCall(RpcContext *ctx);

  unique_ptr<RpcManager> rpc_manager_;

  int num_retries_;
};

unique_ptr<RpcClient> NewRpcClient(shared_ptr<ServerMonitor> monitor,
                                   size_t shard_index,
                                   const ZephyrConfig &config = ZephyrConfig());

} // namespace common
} // namespace zephyr

#endif // ZEPHYR_RPC_CLIENT_H_
