#ifndef ZEPHYR_RPC_MANAGER_H_
#define ZEPHYR_RPC_MANAGER_H_

#include "../../third_party/grpc/third_party/protobuf/src/google/protobuf/message.h"
#include "server_monitor.h"
#include "status.h"
#include "zephyr/utils/imports.h"
#include "zephyr_config.h"

namespace zephyr {
using ::google::protobuf::Message;

namespace common {

class RpcChannel;

struct RpcContext {
  RpcContext(string method, Message *response, DoneCallBack done)
      : method(move(method)), response(response), done(move(done)),
        num_failures(0) {}

  // set and check request proto
  virtual bool Initialize(const Message &request) = 0;

  virtual ~RpcContext() = default;

  string method;
  Message *response;
  DoneCallBack done;
  shared_ptr<RpcChannel> destination;
  int num_failures;
};

class RpcChannel {
public:
  explicit RpcChannel(string host_port) : host_port_(move(host_port)) {}

  virtual ~RpcChannel() = default;

  virtual void IssueRpcCall(RpcContext *ctx) = 0;

  string host_port() { return host_port_; }

private:
  string host_port_;
};

class RpcManager {
public:
  RpcManager()
      : num_channels_per_host_(1), bad_host_cleanup_interval_(1),
        bad_host_timeout_(10), next_replica_index_(0), shutdown_(false),
        bad_hosts_cleaner_(thread(&RpcManager::CleanupBadHosts, this)),
        shard_callback_(
            bind(&RpcManager::AddChannel, this, std::placeholders::_1),
            bind(&RpcManager::RemoveChannel, this, std::placeholders::_1)) {}

  bool Initialize(shared_ptr<ServerMonitor> monitor, size_t shard_index,
                  const ZephyrConfig &config = ZephyrConfig());
  virtual ~RpcManager();

  virtual unique_ptr<RpcChannel> CreateChannel(const string &host_port,
                                               int tag) = 0;

  virtual RpcContext *CreateContext(const string &method, Message *response,
                                    DoneCallBack done) = 0;

  shared_ptr<RpcChannel> GetChannel();

  void MoveToBadHost(const string &host_port);

private:
  // Bad host and detected time.
  using BadHost = pair<string, TimePoint>;

  void AddChannel(const string &host_port);
  void RemoveChannel(const string &host_port);
  void CleanupBadHosts();
  void DoAddChannel(const string &host_port);
  void DoRemoveChannel(const string &host_port);
  void DoCleanupBadHosts(TimePoint now);

  int num_channels_per_host_;
  Duration bad_host_cleanup_interval_;
  Duration bad_host_timeout_;

  vector<shared_ptr<RpcChannel>> channels_;
  vector<BadHost> bad_hosts_;
  size_t next_replica_index_;
  mutex mu_;
  condition_variable cv_;

  bool shutdown_;
  thread bad_hosts_cleaner_;

  shared_ptr<ServerMonitor> monitor_{};
  size_t shard_index_{};
  ShardCallback shard_callback_;
};

} // namespace common
} // namespace zephyr

#endif // ZEPHYR_RPC_MANAGER_H_
