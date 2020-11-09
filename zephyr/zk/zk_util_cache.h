#ifndef ZEPHYR_ZK_UTIL_CACHE_H_
#define ZEPHYR_ZK_UTIL_CACHE_H_

#include "zephyr/utils/imports.h"

namespace zephyr {
namespace zk {

struct ZkInfo {
  ZkInfo(string addr, string path) : addr(move(addr)), path(move(path)) {}

  bool operator==(const ZkInfo &other) const {
    return addr == other.addr && path == other.path;
  }

  string addr;
  string path;
};

struct HashZkInfo {
  size_t operator()(const ZkInfo &zk_info) const {
    return std::hash<string>()(zk_info.addr + ":" + zk_info.path);
  }
};

template <typename ZkUtil>
shared_ptr<ZkUtil> GetOrCreate(const string &zk_addr, const string &zk_path) {
  ZkInfo zk_info(zk_addr, zk_path);

  static mutex zk_utils_mu;
  lock_guard<mutex> lock(zk_utils_mu);

  static unordered_map<ZkInfo, shared_ptr<ZkUtil>, HashZkInfo> zk_utils;
  auto iter = zk_utils.find(zk_info);
  if (iter != zk_utils.end()) {
    return iter->second;
  }

  shared_ptr<ZkUtil> zk_util(new ZkUtil(zk_addr, zk_path));
  if (zk_util->Initialize()) {
    zk_utils.emplace(zk_info, zk_util);
    return zk_util;
  } else {
    return nullptr;
  }
}

} // namespace zk
} // namespace zephyr

#endif // ZEPHYR_ZK_UTIL_CACHE_H_
