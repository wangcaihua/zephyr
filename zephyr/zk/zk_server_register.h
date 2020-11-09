#ifndef ZEPHYR_ZK_SERVER_REGISTER_H_
#define ZEPHYR_ZK_SERVER_REGISTER_H_

#include "zephyr/common/server_register.h"
#include "zephyr/zk/zk_util_cache.h"
#include "zookeeper.h" // NOLINT

namespace zephyr {
namespace zk {
using zephyr::common::ServerRegister;

class ZkServerRegister : public ServerRegister {
  friend shared_ptr<ZkServerRegister>
  GetOrCreate<ZkServerRegister>(const string &, const string &);

public:
  ~ZkServerRegister() override;

  bool RegisterShard(size_t shard_index, const Server &server, const Meta &meta,
                     const Meta &shard_meta) override;
  bool DeregisterShard(size_t shard_index, const Server &server) override;

private:
  static void Watcher(zhandle_t *zh, int type, int state, const char *path,
                      void *data);

  ZkServerRegister(string zk_addr, string zk_path)
      : zk_addr_(move(zk_addr)), zk_path_(move(zk_path)), zk_handle_(nullptr) {}

  bool Initialize() override;

  string zk_addr_;
  string zk_path_;

  mutex zk_mu_;
  zhandle_t *zk_handle_;

  mutex mu_;
  unordered_map<string, string> registered_;
};

} // namespace zk
} // namespace zephyr

#endif // ZEPHYR_ZK_SERVER_REGISTER_H_
