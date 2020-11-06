#include "zephyr/grpc/rpc_client.h"

#include <utility>
#include "zephyr/grpc/impl_register.h"

namespace zephyr {
namespace grpc {

bool RpcClientBase::Initialize(std::shared_ptr<zephyr::zk::ServerMonitor> monitor,
                               size_t shard_index, const zephyr::common::ZephyrConfig &config) {
  config.Get("num_retries", &num_retries_);
  return rpc_manager_ && rpc_manager_->Initialize(monitor, shard_index, config);
}

void RpcClientBase::IssueRpcCall(const std::string &method,
                                 const google::protobuf::Message &request,
                                 google::protobuf::Message *response,
                                 std::function<void(const Status &)> done) {
  RpcContext *ctx = rpc_manager_->CreateContext(method, response, nullptr);
  ctx->done = [ctx, done, this](const Status &status) {
    if (!status.ok()) {
      rpc_manager_->MoveToBadHost(ctx->destination->host_port());
    }

    if (status.ok() ||
        (num_retries_ > 0 && ++ctx->num_failures == num_retries_)) {
      done(status);
      delete ctx;
    } else {
      DoIssueRpcCall(ctx);
    }
  };
  if (!((ctx->Initialize(request)))) {
    done(Status(StatusCode::PROTO_ERROR, "Bad request."));
  }
  DoIssueRpcCall(ctx);
}

void RpcClientBase::DoIssueRpcCall(RpcContext *ctx) {
  ctx->destination = rpc_manager_->GetChannel();
  ctx->destination->IssueRpcCall(ctx);
}

std::unique_ptr<RpcClient> NewRpcClient(
    std::shared_ptr<zephyr::zk::ServerMonitor> monitor, size_t shard_index,
    const zephyr::common::ZephyrConfig &config) {
  std::unique_ptr<RpcClient> rpc_client(ImplFactory<RpcClient>::New());
  if (rpc_client && rpc_client->Initialize(std::move(monitor), shard_index, config)) {
    return rpc_client;
  } else {
    return nullptr;
  }
}

REGISTER_IMPL(RpcClient, RpcClientBase);

}  // namespace grpc
}  // namespace zephyr
