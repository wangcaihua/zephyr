#ifndef ZEPHYR_SERVER_REGISTER_H_
#define ZEPHYR_SERVER_REGISTER_H_

#include <memory>
#include <string>
#include <unordered_map>

namespace zephyr {
namespace zk {

using Meta = std::unordered_map<std::string, std::string>;
using Server = std::string;

class ServerRegister {
 public:
  virtual bool Initialize() = 0;
  virtual ~ServerRegister() = default;

  virtual bool RegisterShard(size_t shard_index, const Server &server,
                             const Meta &meta, const Meta &shard_meta) = 0;
  virtual bool DeregisterShard(size_t shard_index, const Server &server) = 0;
};

std::shared_ptr<ServerRegister> GetServerRegister(const std::string &zk_addr,
                                                  const std::string &zk_path);

}  // namespace zk
}  // namespace zephyr

#endif  // ZEPHYR_SERVER_REGISTER_H_
