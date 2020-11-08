#ifndef ZEPHYR_SERVER_REGISTER_H_
#define ZEPHYR_SERVER_REGISTER_H_

#include "zephyr/utils/imports.h"

namespace zephyr {
namespace common {

class ServerRegister {
public:
  virtual bool Initialize() = 0;
  virtual ~ServerRegister() = default;

  virtual bool RegisterShard(size_t shard_index, const Server &server,
                             const Meta &meta, const Meta &shard_meta) = 0;
  virtual bool DeregisterShard(size_t shard_index, const Server &server) = 0;
};

shared_ptr<ServerRegister> GetServerRegister(const string &zk_addr,
                                             const string &zk_path);

} // namespace common
} // namespace zephyr

#endif // ZEPHYR_SERVER_REGISTER_H_
