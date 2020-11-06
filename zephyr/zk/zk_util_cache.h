#ifndef ZEPHYR_ZK_UTIL_CACHE_H_
#define ZEPHYR_ZK_UTIL_CACHE_H_

#include <functional>
#include <memory>
#include <mutex>  // NOLINT
#include <string>
#include <unordered_map>
#include <utility>

namespace zephyr {
namespace zk {

struct ZkInfo {
  ZkInfo(std::string addr, std::string path)
      : addr(std::move(addr)), path(std::move(path)) { }

  bool operator==(const ZkInfo &other) const {
    return addr == other.addr && path == other.path;
  }

  std::string addr;
  std::string path;
};

struct HashZkInfo {
  size_t operator()(const ZkInfo &zk_info) const {
    return std::hash<std::string>()(zk_info.addr + ":" + zk_info.path);
  }
};

template <typename ZkUtil>
std::shared_ptr<ZkUtil> GetOrCreate(
    const std::string &zk_addr, const std::string &zk_path) {
  ZkInfo zk_info(zk_addr, zk_path);

  static std::mutex zk_utils_mu;
  std::lock_guard<std::mutex> lock(zk_utils_mu);

  static std::unordered_map<ZkInfo, std::shared_ptr<ZkUtil>,
                            HashZkInfo> zk_utils;
  auto iter = zk_utils.find(zk_info);
  if (iter != zk_utils.end()) {
    return iter->second;
  }

  std::shared_ptr<ZkUtil> zk_util(new ZkUtil(zk_addr, zk_path));
  if (zk_util->Initialize()) {
    zk_utils.emplace(zk_info, zk_util);
    return zk_util;
  } else {
    return nullptr;
  }
}

}  // namespace zk
}  // namespace zephyr

#endif  // ZEPHYR_ZK_UTIL_CACHE_H_
