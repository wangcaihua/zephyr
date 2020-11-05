#ifndef ZEPHYR_CONFIG_H_
#define ZEPHYR_CONFIG_H_

#include <string>
#include <map>

namespace zephyr {
namespace common {

class ZephyrConfig {
 public:
  ZephyrConfig();

  bool Load(const std::string& filename);

  bool Get(const std::string& key, std::string* value) const;
  bool Get(const std::string& key, int* value) const;
  void Add(const std::string& key, const std::string& value);
  void Add(const std::string& key, int value);
  void Remove(const std::string& key);

  std::string DebugString() const;

 private:
  std::map<std::string, std::string> config_;
};

}  // namespace common
}  // namespace zephyr

#endif  // ZEPHYR_CONFIG_H_
