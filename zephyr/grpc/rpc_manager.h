#ifndef ZEPHYR_RPC_MANAGER_H_
#define ZEPHYR_RPC_MANAGER_H_

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <utility>

#include "google/protobuf/message.h"
#include "zephyr/common/zephyr_config.h"
#include "zephyr/common/status.h"
#include "zephyr/zk/server_monitor.h"

namespace zephyr {
namespace grpc {

class RpcChannel;

struct RpcContext {
  RpcContext(const std::string &method,
             google::protobuf::Message *response,
             std::function<void(const Status &)> done)
      : method(method), response(response), done(done), num_failures(0) { }

  virtual bool Initialize(const google::protobuf::Message &request) = 0;
  virtual ~RpcContext() = default;

  std::string method;
  google::protobuf::Message *response;
  std::function<void(const Status &)> done;

  std::shared_ptr<RpcChannel> destination;
  int num_failures;
};

class RpcChannel {
 public:
  explicit RpcChannel(const std::string &host_port) : host_port_(host_port) { }

  virtual ~RpcChannel() = default;

  virtual void IssueRpcCall(RpcContext *ctx) = 0;

  std::string host_port() { return host_port_; }

 private:
  std::string host_port_;
};

class RpcManager {
 public:
  RpcManager()
      : num_channels_per_host_(1),
        bad_host_cleanup_interval_(1),
        bad_host_timeout_(10),
        next_replica_index_(0),
        shutdown_(false),
        bad_hosts_cleaner_(std::thread(&RpcManager::CleanupBadHosts, this)),
        shard_callback_(
            std::bind(&RpcManager::AddChannel, this, std::placeholders::_1),
            std::bind(&RpcManager::RemoveChannel, this, std::placeholders::_1)
        ) { }

  bool Initialize(std::shared_ptr<common::ServerMonitor> monitor,
                  size_t shard_index,
                  const GraphConfig &config = GraphConfig());
  virtual ~RpcManager();

  virtual std::unique_ptr<RpcChannel> CreateChannel(
      const std::string &host_port, int tag) = 0;

  virtual RpcContext *CreateContext(
      const std::string &method,
      google::protobuf::Message *respone,
      std::function<void(const Status &)> done) = 0;

  std::shared_ptr<RpcChannel> GetChannel();
  void MoveToBadHost(const std::string &host_port);

 private:
  using TimePoint = std::chrono::time_point<std::chrono::system_clock>;
  using Duration = std::chrono::seconds;
  // Bad host and detected time.
  using BadHost = std::pair<std::string, TimePoint>;

  void AddChannel(const std::string &host_port);
  void RemoveChannel(const std::string &host_port);
  void CleanupBadHosts();
  void DoAddChannel(const std::string &host_port);
  void DoRemoveChannel(const std::string &host_port);
  void DoCleanupBadHosts(TimePoint now);

  int num_channels_per_host_;
  Duration bad_host_cleanup_interval_;
  Duration bad_host_timeout_;

  std::vector<std::shared_ptr<RpcChannel>> channels_;
  std::vector<BadHost> bad_hosts_;
  size_t next_replica_index_;
  std::mutex mu_;
  std::condition_variable cv_;

  bool shutdown_;
  std::thread bad_hosts_cleaner_;

  std::shared_ptr<common::ServerMonitor> monitor_;
  size_t shard_index_;
  common::ShardCallback shard_callback_;
};

}  // namespace grpc
}  // namespace zephyr

#endif  // ZEPHYR_RPC_MANAGER_H_
