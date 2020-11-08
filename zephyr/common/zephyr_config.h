#ifndef ZEPHYR_CONFIG_H_
#define ZEPHYR_CONFIG_H_

#include "zephyr/utils/imports.h"

namespace zephyr {
namespace common {

class ZephyrConfig {
public:
  ZephyrConfig();

  bool Load(const string &filename);

  bool Get(const string &key, string *value) const;
  bool Get(const string &key, int *value) const;
  bool Get(const string &key, Duration *value) const;
  void Add(const string &key, const string &value);
  void Add(const string &key, int value);
  void Remove(const string &key);

  string DebugString() const;

private:
  map<string, string> config_;
};

} // namespace common
} // namespace zephyr

#endif // ZEPHYR_CONFIG_H_
