#ifndef ZEPHYR_SERVER_MONITOR_H_
#define ZEPHYR_SERVER_MONITOR_H_

#include "zephyr/utils/imports.h"

namespace zephyr {
namespace common {

struct ShardCallback {
  ShardCallback(MonitorCallBack on_add_server, MonitorCallBack on_remove_server)
      : on_add_server(move(on_add_server)),
        on_remove_server(move(on_remove_server)) {}

  MonitorCallBack on_add_server;
  MonitorCallBack on_remove_server;
};

class ServerMonitor {
public:
  virtual bool Initialize() = 0;
  virtual ~ServerMonitor() = default;

  virtual bool GetMeta(const string &key, string *value) = 0;
  virtual bool GetNumShards(int *value) = 0;
  virtual bool GetShardMeta(size_t shard_index, const string &key,
                            string *value) = 0;
  virtual bool SetShardCallback(size_t shard_index,
                                const ShardCallback *callback) = 0;
  virtual bool UnsetShardCallback(size_t shard_index,
                                  const ShardCallback *callback) = 0;
};

class ServerMonitorBase : public ServerMonitor {
public:
  bool GetMeta(const string &key, string *value) override;
  bool GetNumShards(int *value) override;
  bool GetShardMeta(size_t shard_index, const string &key,
                    string *value) override;
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
  static bool GetMeta(const Meta &meta, const string &key, string *value);
  static void UpdateMeta(const Meta &new_meta, unique_ptr<Meta> *meta);

  struct ShardInfo {
    unique_ptr<Meta> meta;
    unordered_set<Server> servers;
    unordered_set<const ShardCallback *> callbacks;
  };

  unique_ptr<Meta> meta_;
  unordered_map<size_t, ShardInfo> shards_;

  mutex mu_;
  condition_variable cv_;
};

shared_ptr<ServerMonitor> GetServerMonitor(const string &zk_addr,
                                           const string &zk_path);

} // namespace common
} // namespace zephyr

#endif // ZEPHYR_SERVER_MONITOR_H_
