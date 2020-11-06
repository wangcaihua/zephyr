#include "zephyr_config.h"

#include <stdio.h>

#include <string>
#include <vector>

#include "glog/logging.h"
#include "zephyr/common/string_util.h"

namespace zephyr {
namespace common {

ZephyrConfig::ZephyrConfig() = default;

bool ZephyrConfig::Load(const std::string& filename) {
  FILE* fp = fopen(filename.c_str(), "rb");
  if (fp == nullptr) {
    LOG(INFO) << "Open zephyr config file: " << filename << "failed!";
    return false;
  }
  char* line = nullptr;
  size_t n = 0;
  ssize_t nread = 0;
  while ((nread = getline(&line, &n, fp)) > 0) {
    line[nread] = '\0';
    std::vector<std::string> vec;
    zephyr::common::split_string(line, '=', &vec);
    if (vec.size() != 2) {
      continue;
    }
    Add(zephyr::common::trim("\t\r ", vec[0]),
        zephyr::common::trim("\t\r ", vec[1]));
  }
  free(line);
  fclose(fp);
  return true;
}

bool ZephyrConfig::Get(const std::string& key, std::string* value) const {
  auto it = config_.find(key);
  if (it != config_.end()) {
    *value = it->second;
    return true;
  }
  return false;
}

bool ZephyrConfig::Get(const std::string& key, int* value) const {
  std::string value_string;
  if (!Get(key, &value_string)) {
    return false;
  }

  try {
    *value = std::stoi(value_string);
    return true;
  } catch (std::invalid_argument e) {
  }
  return false;
}

void ZephyrConfig::Add(const std::string& key,
                      const std::string& value) {
  config_.insert({key, value});
}

void ZephyrConfig::Add(const std::string& key, int value) {
  Add(key, std::to_string(value));
}

void ZephyrConfig::Remove(const std::string& key) {
  config_.erase(key);
}

std::string ZephyrConfig::DebugString() const {
  std::string out = "{\n";
  for (const auto & it : config_) {
    out.append(it.first);
    out.append(" = ");
    out.append(it.second);
    out.append("\n");
  }
  out.append("}");
  return out;
}

}  // namespace common
}  // namespace zephyr
