#ifndef ZEPHYR_ZK_SERVER_MONITOR_H_
#define ZEPHYR_ZK_SERVER_MONITOR_H_

#include <unordered_set>
#include <string>
#include <utility>

#include "zookeeper.h"  // NOLINT

#include "zephyr/zk/server_monitor.h"
#include "zephyr/zk/zk_util_cache.h"

namespace zephyr {
namespace zk {

class ZkServerMonitor : public ServerMonitorBase {
  friend std::shared_ptr<ZkServerMonitor> GetOrCreate<ZkServerMonitor>(
      const std::string &, const std::string &);

 public:
  ~ZkServerMonitor() override;

 private:
  static void Watcher(zhandle_t *zh, int type, int state, const char *path,
                      void *data);
  static void RootCallback(int rc, const struct Stat *stat, const void *data);
  static void RootWatcher(zhandle_t *zk_handle, int type, int state,
                          const char *path, void *data);
  static void ChildCallback(int rc, const struct String_vector *strings,
                            const void *data);
  static void ChildWatcher(zhandle_t *zh, int type, int state,
                           const char *path, void *data);
  static void MetaCallback(int rc, const char *value, int value_len,
                           const struct Stat *stat, const void *data);

  ZkServerMonitor(std::string zk_addr, std::string zk_path)
    : zk_addr_(std::move(zk_addr)), zk_path_(std::move(zk_path)), zk_handle_(nullptr) { }

  bool Initialize() override;

  void OnAddChild(const std::string &child);
  void OnRemoveChild(const std::string &child);

  std::string zk_addr_;
  std::string zk_path_;

  std::mutex zk_mu_;
  zhandle_t *zk_handle_;
  std::unordered_set<std::string> children_;
};

}  // namespace zk
}  // namespace zephyr

#endif  // ZEPHYR_ZK_SERVER_MONITOR_H_
