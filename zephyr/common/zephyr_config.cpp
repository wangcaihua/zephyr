#include "zephyr_config.h"

#include "glog/logging.h"

namespace zephyr {
namespace common {

ZephyrConfig::ZephyrConfig() = default;

bool ZephyrConfig::Load(const string &filename) {
  FILE *fp = fopen(filename.c_str(), "rb");
  if (fp == nullptr) {
    LOG(INFO) << "Open zephyr config file: " << filename << "failed!";
    return false;
  }
  char *line = nullptr;
  size_t n = 0;
  ssize_t nread = 0;
  while ((nread = getline(&line, &n, fp)) > 0) {
    line[nread] = '\0';
    vector<string> vec;
    split_string(line, '=', &vec);
    if (vec.size() != 2) {
      continue;
    }
    Add(trim("\t\r ", vec[0]), trim("\t\r ", vec[1]));
  }
  free(line);
  fclose(fp);
  return true;
}

bool ZephyrConfig::Get(const string &key, string *value) const {
  auto it = config_.find(key);
  if (it != config_.end()) {
    *value = it->second;
    return true;
  }
  return false;
}

bool ZephyrConfig::Get(const string &key, int *value) const {
  string value_string;
  if (!Get(key, &value_string)) {
    return false;
  }

  try {
    *value = stoi(value_string);
    return true;
  } catch (std::invalid_argument e) {
  }
  return false;
}

bool ZephyrConfig::Get(const string &key, Duration *value) const {
  string value_string;
  if (!Get(key, &value_string)) {
    return false;
  }

  try {
    *value = Duration(stoi(value_string));
    return true;
  } catch (std::invalid_argument e) {
  }
  return false;
}

void ZephyrConfig::Add(const string &key, const string &value) {
  config_.insert({key, value});
}

void ZephyrConfig::Add(const string &key, int value) {
  Add(key, std::to_string(value));
}

void ZephyrConfig::Remove(const string &key) { config_.erase(key); }

string ZephyrConfig::DebugString() const {
  string out = "{\n";
  for (const auto &it : config_) {
    out.append(it.first);
    out.append(" = ");
    out.append(it.second);
    out.append("\n");
  }
  out.append("}");
  return out;
}

} // namespace common
} // namespace zephyr
