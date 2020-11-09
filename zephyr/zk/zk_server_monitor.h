#ifndef ZEPHYR_ZK_SERVER_MONITOR_H_
#define ZEPHYR_ZK_SERVER_MONITOR_H_

#include <string>
#include <unordered_set>
#include <utility>

#include "zookeeper.h" // NOLINT

#include "zephyr/common/server_monitor.h"
#include "zephyr/zk/zk_util_cache.h"

namespace zephyr {
namespace zk {
using zephyr::common::ServerMonitorBase;

class ZkServerMonitor : public ServerMonitorBase {
  friend shared_ptr<ZkServerMonitor>
  GetOrCreate<ZkServerMonitor>(const string &, const string &);

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

  static void ChildWatcher(zhandle_t *zh, int type, int state, const char *path,
                           void *data);

  static void MetaCallback(int rc, const char *value, int value_len,
                           const struct Stat *stat, const void *data);

  ZkServerMonitor(string zk_addr, string zk_path)
      : zk_addr_(move(zk_addr)), zk_path_(move(zk_path)), zk_handle_(nullptr) {}

  bool Initialize() override;

  void OnAddChild(const string &child);

  void OnRemoveChild(const string &child);

  string zk_addr_;
  string zk_path_;

  mutex zk_mu_;
  zhandle_t *zk_handle_;
  unordered_set<string> children_;
};

} // namespace zk
} // namespace zephyr

#endif // ZEPHYR_ZK_SERVER_MONITOR_H_
