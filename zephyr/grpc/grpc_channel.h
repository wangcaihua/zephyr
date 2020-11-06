#ifndef ZEPHYR_GRPC_CHANNEL_H_
#define ZEPHYR_GRPC_CHANNEL_H_

#include <string>

#include "grpcpp/generic/generic_stub.h"
#include "grpcpp/grpcpp.h"
#include "zephyr/grpc/rpc_manager.h"

namespace zephyr {
namespace grpc {

struct GrpcContext: public RpcContext {
  GrpcContext(const std::string &method,
              google::protobuf::Message *response,
              std::function<void(const Status &)> done)
      : RpcContext(method, response, done) { }

  bool Initialize(const google::protobuf::Message &request);

  grpc::ByteBuffer request_buf;
  grpc::ByteBuffer response_buf;
  grpc::Status status;
  std::unique_ptr<grpc::ClientContext> context;
  std::unique_ptr<grpc::GenericClientAsyncResponseReader> response_reader;
};

class GrpcChannel: public RpcChannel {
 public:
  GrpcChannel(const std::string& host_port,
              std::shared_ptr<grpc::Channel> raw_channel);

  void IssueRpcCall(RpcContext *ctx) override;

 private:
  grpc::GenericStub stub_;
  grpc::CompletionQueue *cq_;
};

}  // namespace grpc
}  // namespace zephyr

#endif  // ZEPHYR_GRPC_CHANNEL_H_
