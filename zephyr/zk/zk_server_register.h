#ifndef ZEPHYR_ZK_SERVER_REGISTER_H_
#define ZEPHYR_ZK_SERVER_REGISTER_H_

#include <unordered_map>
#include <string>
#include <mutex>  // NOLINT
#include <utility>

#include "zephyr/common/server_register.h"
#include "zephyr/zk/zk_util_cache.h"
#include "zookeeper.h" // NOLINT

namespace zephyr {
namespace zk {

class ZkServerRegister : public ServerRegister {
  friend std::shared_ptr<ZkServerRegister> GetOrCreate<ZkServerRegister>(
      const std::string &, const std::string &);

 public:
  ~ZkServerRegister() override;

  bool RegisterShard(size_t shard_index, const Server &server,
                     const Meta &meta, const Meta &shard_meta) override;
  bool DeregisterShard(size_t shard_index, const Server &server) override;

 private:
  static void Watcher(zhandle_t *zh, int type, int state, const char *path,
                      void *data);

  ZkServerRegister(std::string zk_addr, std::string zk_path)
      : zk_addr_(std::move(zk_addr)), zk_path_(std::move(zk_path)), zk_handle_(nullptr) { }

  bool Initialize() override;

  std::string zk_addr_;
  std::string zk_path_;

  std::mutex zk_mu_;
  zhandle_t *zk_handle_;

  std::mutex mu_;
  std::unordered_map<std::string, std::string> registered_;
};

}  // namespace zk
}  // namespace zephyr

#endif  // ZEPHYR_ZK_SERVER_REGISTER_H_
