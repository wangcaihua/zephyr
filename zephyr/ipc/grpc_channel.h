#ifndef ZEPHYR_GRPC_CHANNEL_H_
#define ZEPHYR_GRPC_CHANNEL_H_

#include "grpcpp/generic/generic_stub.h"
#include "zephyr/ipc/grpc_manager.h"

namespace zephyr {
namespace grpc {

using ::grpc::ByteBuffer;
using ::grpc::CompletionQueue;
using ::grpc::GenericStub;
using GrpcStatus = ::grpc::Status;
using ::grpc::GenericClientAsyncResponseReader;

struct GrpcContext : public RpcContext {
  GrpcContext(const string &method, Message *response, DoneCallBack done)
      : RpcContext(method, response, move(done)) {}

  bool Initialize(const Message &request) override;

  ByteBuffer request_buf;
  ByteBuffer response_buf;
  GrpcStatus status;
  unique_ptr<ClientContext> context;
  unique_ptr<GenericClientAsyncResponseReader> response_reader;
};

class GrpcChannel : public RpcChannel {
public:
  GrpcChannel(const string &host_port, const shared_ptr<Channel>& raw_channel);

  void IssueRpcCall(RpcContext *ctx) override;

private:
  GenericStub stub_;
  CompletionQueue *cq_;
};

} // namespace grpc
} // namespace zephyr

#endif // ZEPHYR_GRPC_CHANNEL_H_
