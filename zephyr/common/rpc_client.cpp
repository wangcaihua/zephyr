#include "zephyr/common/rpc_client.h"

#include "impl_register.h"

namespace zephyr {
namespace common {

bool RpcClientBase::Initialize(shared_ptr<ServerMonitor> monitor,
                               size_t shard_index, const ZephyrConfig &config) {
  config.Get("num_retries", &num_retries_);
  return rpc_manager_ && rpc_manager_->Initialize(monitor, shard_index, config);
}

void RpcClientBase::IssueRpcCall(const string &method, const Message &request,
                                 Message *response, DoneCallBack done) {
  // 1) create a context for rpc call
  RpcContext *ctx = rpc_manager_->CreateContext(method, response, nullptr);

  // 2) define ctx->done, which is different from DoneCallBack
  ctx->done = [ctx, done, this](const Status &status) {
    // if not OK(INVALID_ARGUMENT, PROTO_ERROR, RPC_ERROR),
    // then move the host to BadHost list
    if (!status.ok()) {
      rpc_manager_->MoveToBadHost(ctx->destination->host_port());
    }

    if (status.ok() ||
        (num_retries_ > 0 && ++ctx->num_failures == num_retries_)) {
      // OK, reach the max retry
      done(status);
      delete ctx;
    } else { // OK, issue Rpc call
      DoIssueRpcCall(ctx);
    }
  };

  // 3) set and check request
  if (!(ctx->Initialize(request))) {
    done(Status(StatusCode::PROTO_ERROR, "Bad request."));
  }

  DoIssueRpcCall(ctx);
}

void RpcClientBase::DoIssueRpcCall(RpcContext *ctx) {
  ctx->destination = rpc_manager_->GetChannel();
  ctx->destination->IssueRpcCall(ctx);
}

unique_ptr<RpcClient> NewRpcClient(shared_ptr<ServerMonitor> monitor,
                                   size_t shard_index,
                                   const ZephyrConfig &config) {
  unique_ptr<RpcClient> rpc_client(ImplFactory<RpcClient>::New());
  if (rpc_client &&
      rpc_client->Initialize(move(monitor), shard_index, config)) {
    return rpc_client;
  } else {
    return nullptr;
  }
}

REGISTER_IMPL(RpcClient, RpcClientBase);

} // namespace common
} // namespace zephyr
