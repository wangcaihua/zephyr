#ifndef ZEPHYR_SERVER_MONITOR_H_
#define ZEPHYR_SERVER_MONITOR_H_

#include <condition_variable>  // NOLINT
#include <functional>
#include <map>
#include <memory>
#include <mutex>  // NOLINT
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace zephyr {
namespace zk {

using Meta = std::unordered_map<std::string, std::string>;
using Server = std::string;

struct ShardCallback {
  ShardCallback(std::function<void(const Server &)> on_add_server,
                std::function<void(const Server &)> on_remove_server)
      : on_add_server(std::move(on_add_server)), on_remove_server(std::move(on_remove_server)) { }

  std::function<void(const Server &)> on_add_server;
  std::function<void(const Server &)> on_remove_server;
};

class ServerMonitor {
 public:
  virtual bool Initialize() = 0;
  virtual ~ServerMonitor() = default;

  virtual bool GetMeta(const std::string &key, std::string *value) = 0;
  virtual bool GetNumShards(int *value) = 0;
  virtual bool GetShardMeta(size_t shard_index, const std::string &key,
                            std::string *value) = 0;
  virtual bool SetShardCallback(size_t shard_index,
                                const ShardCallback *callback) = 0;
  virtual bool UnsetShardCallback(size_t shard_index,
                                  const ShardCallback *callback) = 0;

};

class ServerMonitorBase : public ServerMonitor {
 public:
  bool GetMeta(const std::string &key, std::string *value) override;
  bool GetNumShards(int *value) override;
  bool GetShardMeta(size_t shard_index, const std::string &key,
                    std::string *value) override;
  bool SetShardCallback(size_t shard_index,
                        const ShardCallback *callback) override;
  bool UnsetShardCallback(size_t shard_index,
                          const ShardCallback *callback) override;

 protected:
  void UpdateMeta(const Meta &new_meta);
  void UpdateShardMeta(size_t shard_index, const Meta &new_meta);
  void AddShardServer(size_t shard_index, const Server &server);
  void RemoveShardServer(size_t shard_index, const Server &server);

 private:
  static bool GetMeta(const Meta &meta, const std::string &key,
                      std::string *value);
  static void UpdateMeta(const Meta &new_meta, std::unique_ptr<Meta> *meta);

  struct ShardInfo {
    std::unique_ptr<Meta> meta;
    std::unordered_set<Server> servers;
    std::unordered_set<const ShardCallback *> callbacks;
  };

  std::unique_ptr<Meta> meta_;
  std::unordered_map<size_t, ShardInfo> shards_;

  std::mutex mu_;
  std::condition_variable cv_;
};

std::shared_ptr<ServerMonitor> GetServerMonitor(const std::string &zk_addr,
                                                const std::string &zk_path);

}  // namespace zk
}  // namespace zephyr

#endif  // ZEPHYR_SERVER_MONITOR_H_
