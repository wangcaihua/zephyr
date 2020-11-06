#include "zephyr/zk/zk_server_monitor.h"

#include <vector>

#include "glog/logging.h"
#include "zephyr/common/string_util.h"
#include "zephyr/zk/zk_util_cache.h"


using zephyr::common::split_string;
using zephyr::common::join_string;

namespace zephyr {
namespace zk {
namespace {
bool BytesToMeta(const std::string &bytes, Meta *meta, Meta *shard_meta) {
  if (bytes.empty()) {
    return true;
  }

  std::vector<std::string> lines;
  zephyr::common::split_string(bytes, '\n', &lines);
  for (const std::string &line : lines) {
    std::vector<std::string> parts;
    int num_parts = split_string(line, ':', &parts);
    if (num_parts == 2) {
      meta->emplace(std::move(parts[0]), std::move(parts[1]));
    } else if (num_parts == 3) {
      shard_meta->emplace(std::move(parts[1]), std::move(parts[2]));
    } else {
      LOG(WARNING) << "Invalid meta information: " << line << ".";
    }
  }
  return true;
}

bool BytesToShard(const std::string &bytes,
                  size_t *shard_index, Server *server) {
  std::vector<std::string> parts;
  if (split_string(bytes, '#', &parts) != 2) {
    return false;
  }

  try {
    *shard_index = std::stoul(parts[0]);
  } catch (std::invalid_argument e) {
    return false;
  }
  *server = std::move(parts[1]);
  return true;
}

template <typename t>
void SetDifference(const std::unordered_set<t> &input1,
                    const std::unordered_set<t> &input2,
                    std::function<void(const t &)> fn) {
  for (const t &element : input1) {
    if (input2.find(element) == input2.end()) {
      fn(element);
    }
  }
}

void ZkLogCallback(const char * /*message*/) { }

}  // namespace

bool ZkServerMonitor::Initialize() {
  {
    std::lock_guard<std::mutex> lock(zk_mu_);

    if (zk_handle_) {
      return true;
    }

    zhandle_t *zh = zookeeper_init2(zk_addr_.c_str(), Watcher, 60000, nullptr,
                                    this, 0, ZkLogCallback);
    if (zh == nullptr) {
      LOG(ERROR) << "Fail to initialize ZK connection.";
      return false;
    }
    zk_handle_ = zh;
  }

  int rc = zoo_awexists(zk_handle_, zk_path_.c_str(), RootWatcher, this,
                        RootCallback, this);
  if (rc != ZOK) {
    LOG(ERROR) << "ZK error when checking root node: " << zerror(rc) << ".";
  }

  return true;
}

ZkServerMonitor::~ZkServerMonitor() {
  std::lock_guard<std::mutex> lock(zk_mu_);
  zookeeper_close(zk_handle_);
  zk_handle_ = nullptr;
}

void ZkServerMonitor::Watcher(
    zhandle_t *zh, int /*type*/, int state, const char * /*path*/, void *data) {
  if (state == ZOO_EXPIRED_SESSION_STATE) {
    zookeeper_close(zh);

    auto *self = static_cast<ZkServerMonitor *>(data);
    {
      std::lock_guard<std::mutex> lock(self->zk_mu_);

      self->zk_handle_ = nullptr;
      while (self->zk_handle_ == nullptr) {
        LOG(WARNING) << "Reconnecting ZK ...";
        self->zk_handle_ = zookeeper_init2(self->zk_addr_.c_str(), Watcher,
                                           60000, nullptr, self, 0,
                                           ZkLogCallback);
      }
    }

    int rc = zoo_awexists(self->zk_handle_, self->zk_path_.c_str(), RootWatcher,
                          self, RootCallback, self);
    if (rc != ZOK) {
      LOG(ERROR) << "ZK error when checking root node: " << zerror(rc) << ".";
    }
  }
}

void ZkServerMonitor::RootCallback(
    int rc, const struct Stat * /*stat*/, const void *data) {
  if (rc == ZOK) {
    auto* self = (ZkServerMonitor *) data;
    int rc = zoo_awget_children(self->zk_handle_, self->zk_path_.c_str(),
                                ChildWatcher, self, ChildCallback, self);
    if (rc != ZOK) {
      LOG(ERROR) << "ZK error when watching child: " << zerror(rc) << ".";
    }
  } else if (rc == ZNONODE) {
  } else {
    LOG(ERROR) << "ZK error when checking root node: " << zerror(rc) << ".";
  }
}

void ZkServerMonitor::RootWatcher(
    zhandle_t * /*zh*/, int type, int /*state*/, const char * /*path*/, void *data) {
  if (type == ZOO_CREATED_EVENT) {
    auto *self = (ZkServerMonitor *) data;
    int rc = zoo_awget_children(self->zk_handle_, self->zk_path_.c_str(),
                                ChildWatcher, self, ChildCallback, self);
    if (rc != ZOK) {
      LOG(ERROR) << "ZK error when watching child: " << zerror(rc) << ".";
    }
  } else if (type == ZOO_CHANGED_EVENT || ZOO_DELETED_EVENT) {
    // These events may be fired due to meta node already exist when first
    // queried with zoo_wexists, then changed for some reason. Low version of
    // zookeeper doesn't support watch removal.
    LOG(INFO) << "ZK receive watch event on root with code: " << type << ".";
  } else {
    // TODO: session event.
  }
}

void ZkServerMonitor::ChildCallback(
    int rc, const struct String_vector *strings, const void *data) {
  if (rc == ZOK) {
    auto *self = (ZkServerMonitor *) data;
    std::unordered_set<std::string> new_children(
        strings->data, strings->data + strings->count);

    using namespace std::placeholders;
    SetDifference<std::string>(
        new_children, self->children_,
        std::bind(&ZkServerMonitor::OnAddChild, self, _1));
    SetDifference<std::string>(
        self->children_, new_children,
        std::bind(&ZkServerMonitor::OnRemoveChild, self, _1));

    self->children_ = std::move(new_children);
  } else if (rc == ZNONODE) {
    // ZOO_DELETED_EVENT should be fired to watcher.
    LOG(WARNING) << "ZK root node get deleted.";
  } else {
    LOG(ERROR) << "ZK error when watching root node: " << zerror(rc) << ".";
  }
}

void ZkServerMonitor::ChildWatcher(
    zhandle_t * /*zh*/, int type, int /*state*/, const char * /*path*/,
    void *data) {
  auto *self = static_cast<ZkServerMonitor *>(data);
  if (type == ZOO_CHILD_EVENT) {
    int rc = zoo_awget_children(self->zk_handle_, self->zk_path_.c_str(),
                                ChildWatcher, self, ChildCallback, self);
    if (rc != ZOK) {
      LOG(ERROR) << "ZK error when watching root node: " << zerror(rc) << ".";
    }
  } else if (type == ZOO_DELETED_EVENT) {
    int rc = zoo_awexists(self->zk_handle_, self->zk_path_.c_str(), RootWatcher,
                          self, RootCallback, self);
    if (rc != ZOK) {
      LOG(ERROR) << "ZK error when checking root node: " << zerror(rc) << ".";
    }
  }
}

using ZkShardClosure = std::pair<ZkServerMonitor *, size_t>;

void ZkServerMonitor::MetaCallback(
    int rc, const char *value, int value_len, const struct Stat * /*stat*/,
    const void *data) {
  if (rc == ZOK) {
    std::unique_ptr<ZkShardClosure> closure((ZkShardClosure *) data);
    ZkServerMonitor *self = closure->first;
    size_t shard_index = closure->second;
    std::string meta_bytes(value, value_len);
    Meta meta, shard_meta;
    BytesToMeta(meta_bytes, &meta, &shard_meta);
    self->UpdateMeta(meta);
    self->UpdateShardMeta(shard_index, shard_meta);
  } else if (rc == ZNONODE) {
  } else {
    LOG(ERROR) << "ZK error when reading meta: " << zerror(rc) << ".";
  }
}

void ZkServerMonitor::OnAddChild(const std::string &child) {
  LOG(INFO) << "Online node: " << child << ".";

  size_t shard_index;
  Server server;
  if (BytesToShard(child, &shard_index, &server)) {
    zoo_aget(zk_handle_, join_string({zk_path_, child}, "/").c_str(), 0,
             MetaCallback, new ZkShardClosure(this, shard_index));

    AddShardServer(shard_index, server);
  } else {
    LOG(WARNING) << "Invalid ZK child: " << child << ".";
  }
}

void ZkServerMonitor::OnRemoveChild(const std::string &child) {
  LOG(INFO) << "Offline node: " << child << ".";

  size_t shard_index;
  Server server;
  if (BytesToShard(child, &shard_index, &server)) {
    RemoveShardServer(shard_index, server);
  }
}

std::shared_ptr<ServerMonitor> GetServerMonitor(const std::string& zk_addr,
                                                const std::string& zk_path) {
  return GetOrCreate<ZkServerMonitor>(zk_addr, zk_path);
}

}  // namespace zk
}  // namespace zephyr
