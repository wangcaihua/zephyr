#include "zephyr/common/string_util.h"

#include <sstream>
#include <string>

namespace zephyr {
namespace common {

std::string& ltrim(const std::string& chars,
                   std::string& str) {
  str.erase(0, str.find_first_not_of(chars));
  return str;
}

std::string& rtrim(const std::string& chars,
                   std::string& str) {
  str.erase(str.find_last_not_of(chars) + 1);
  return str;
}

std::string& trim(const std::string& chars, std::string& str) {
  return ltrim(chars, rtrim(chars, str));
}

int32_t split_string(const std::string &s, char delim,
                     std::vector<std::string>* v) {
  if (v == nullptr)
    return 0;
  v->clear();
  size_t pos = s.find(delim);
  size_t beg = 0;
  while (pos != std::string::npos) {
    v->push_back(std::string(s, beg, pos - beg));
    beg = pos + 1;
    pos = s.find(delim, beg);
  }
  v->push_back(std::string(s, beg));
  return v->size();
}

std::string join_string(const std::vector<std::string> &parts,
                        const std::string &separator) {
  std::stringstream ss;
  for (size_t i = 0; i < parts.size(); ++i) {
    if (i > 0) {
      ss << separator;
    }
    ss << parts[i];
  }
  return ss.str();
}

}  // namespace common
}  // namespace zephyr
