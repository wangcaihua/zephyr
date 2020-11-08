#include "rpc_manager.h"
#include "glog/logging.h"
#include "impl_register.h"

namespace zephyr {
namespace common {

bool RpcManager::Initialize(shared_ptr<ServerMonitor> monitor,
                            size_t shard_index, const ZephyrConfig &config) {
  if (monitor_) {
    return true;
  }

  config.Get("num_channels_per_host", &num_channels_per_host_);
  config.Get("bad_host_cleanup_interval", &bad_host_cleanup_interval_);
  config.Get("bad_host_timeout", &bad_host_timeout_);

  // set call back
  bool success = monitor->SetShardCallback(shard_index, &shard_callback_);
  if (success) {
    monitor_ = move(monitor);
    shard_index_ = shard_index;
  } else {
    LOG(ERROR) << "Fail to listen on ServerMonitor.";
  }
  return success;
}

RpcManager::~RpcManager() {
  shutdown_ = true;
  bad_hosts_cleaner_.join();
  if (monitor_) {
    monitor_->UnsetShardCallback(shard_index_, &shard_callback_);
  }
}

shared_ptr<RpcChannel> RpcManager::GetChannel() {
  unique_lock<mutex> lock(mu_);
  cv_.wait(lock, [this] { return !channels_.empty(); });
  return channels_[next_replica_index_++ % channels_.size()];
}

void RpcManager::MoveToBadHost(const string &host_port) {
  lock_guard<mutex> lock(mu_);
  // 1) remove all channels connect to `host_port`
  DoRemoveChannel(host_port);

  // 2) if `host_port` is not in bad_hosts_, then add to bad_hosts_.
  if (find_if(bad_hosts_.begin(), bad_hosts_.end(),
              [host_port](const BadHost &bad_host) {
                return bad_host.first == host_port;
              }) == bad_hosts_.end()) {
    bad_hosts_.emplace_back(host_port, std::chrono::system_clock::now());
  }
}

void RpcManager::AddChannel(const string &host_port) {
  {
    lock_guard<mutex> lock(mu_);
    DoAddChannel(host_port);
  }
  cv_.notify_all();
}

void RpcManager::RemoveChannel(const string &host_port) {
  lock_guard<mutex> lock(mu_);
  DoRemoveChannel(host_port);
  bad_hosts_.erase(remove_if(bad_hosts_.begin(), bad_hosts_.end(),
                             [host_port](const BadHost &bad_host) {
                               return bad_host.first == host_port;
                             }),
                   bad_hosts_.end());
}

void RpcManager::CleanupBadHosts() {
  while (!shutdown_) {
    std::this_thread::sleep_for(bad_host_cleanup_interval_);
    TimePoint now = std::chrono::system_clock::now();
    {
      lock_guard<mutex> lock(mu_);
      DoCleanupBadHosts(now);
    }
    cv_.notify_all();
  }
}

void RpcManager::DoAddChannel(const string &host_port) {
  for (int tag = 0; tag < num_channels_per_host_; ++tag) {
    channels_.emplace_back(CreateChannel(host_port, tag));
  }
}

void RpcManager::DoRemoveChannel(const string &host_port) {
  channels_.erase(remove_if(channels_.begin(), channels_.end(),
                            [host_port](const shared_ptr<RpcChannel> &channel) {
                              return channel->host_port() == host_port;
                            }),
                  channels_.end());
}

void RpcManager::DoCleanupBadHosts(TimePoint now) {
  // 1) find all alive hosts
  auto iter = partition(bad_hosts_.begin(), bad_hosts_.end(),
                        [now, this](const BadHost &bad_host) {
                          return now - bad_host.second < bad_host_timeout_;
                        });
  // 2) add channels for each alive host
  for_each(iter, bad_hosts_.end(),
           [this](const BadHost &bad_host) { DoAddChannel(bad_host.first); });

  // 3) remove all alive host from bad_hosts
  bad_hosts_.erase(iter, bad_hosts_.end());
}

} // namespace common
} // namespace zephyr
