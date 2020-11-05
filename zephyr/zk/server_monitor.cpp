#include "zephyr/zk/server_monitor.h"

namespace zephyr {
namespace zk {

bool ServerMonitorBase::GetMeta(const Meta &meta, const std::string &key,
                                std::string *value) {
  auto iter = meta.find(key);
  if (iter == meta.end()) {
    return false;
  }
  *value = iter->second;
  return true;
}

bool ServerMonitorBase::GetMeta(const std::string &key, std::string *value) {
  std::unique_lock<std::mutex> lock(mu_);
  cv_.wait(lock, [this] { return static_cast<bool>(meta_); });
  return GetMeta(*meta_, key, value);
}

bool ServerMonitorBase::GetNumShards(int *value) {
  std::string value_string;
  if (!GetMeta("num_shards", &value_string)) {
    return false;
  } else {
    try {
      *value = std::stoul(value_string);
    } catch (std::invalid_argument e) {
      return false;
    }
    return true;
  }
}

bool ServerMonitorBase::GetShardMeta(size_t shard_index, const std::string &key,
                                     std::string *value) {
  std::unique_lock<std::mutex> lock(mu_);
  cv_.wait(lock, [this, shard_index]{
    auto shard = shards_.find(shard_index);
    return shard != shards_.end() && shard->second.meta;
  });
  return GetMeta(*shards_[shard_index].meta, key, value);
}

bool ServerMonitorBase::SetShardCallback(
    size_t shard_index, const ShardCallback *callback) {
  std::lock_guard<std::mutex> lock(mu_);
  ShardInfo &shard = shards_[shard_index];
  if (!shard.callbacks.emplace(callback).second) {
    return false;
  }

  for (const Server &server : shard.servers) {
    callback->on_add_server(server);
  }
  return true;
}

bool ServerMonitorBase::UnsetShardCallback(
    size_t shard_index, const ShardCallback *callback) {
  std::lock_guard<std::mutex> lock(mu_);
  ShardInfo &shard = shards_[shard_index];
  return shard.callbacks.erase(callback) > 0;
}

void ServerMonitorBase::UpdateMeta(const Meta &new_meta,
                                   std::unique_ptr<Meta> *meta) {
  if (!*meta) {
    meta->reset(new Meta);
  }
  **meta = new_meta;
}

void ServerMonitorBase::UpdateMeta(const Meta &new_meta) {
  std::lock_guard<std::mutex> lock(mu_);
  UpdateMeta(new_meta, &meta_);
  cv_.notify_all();
}

void ServerMonitorBase::UpdateShardMeta(size_t shard_index,
                                        const Meta &new_meta) {
  std::lock_guard<std::mutex> lock(mu_);
  ShardInfo &shard = shards_[shard_index];
  UpdateMeta(new_meta, &shard.meta);
  cv_.notify_all();
}

void ServerMonitorBase::AddShardServer(size_t shard_index,
                                       const Server &server) {
  std::lock_guard<std::mutex> lock(mu_);
  ShardInfo &shard = shards_[shard_index];
  for (const auto callback : shard.callbacks) {
    callback->on_add_server(server);
  }
  shard.servers.emplace(server);
}

void ServerMonitorBase::RemoveShardServer(size_t shard_index,
                                          const Server &server) {
  std::lock_guard<std::mutex> lock(mu_);
  ShardInfo &shard = shards_[shard_index];
  for (const auto callback : shard.callbacks) {
    callback->on_remove_server(server);
  }
  shard.servers.erase(server);
}

}  // namespace zk
}  // namespace zephyr
