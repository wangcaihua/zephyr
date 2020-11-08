#ifndef ZEPHYR_STRING_UTIL_H_
#define ZEPHYR_STRING_UTIL_H_

#include <cstdint>
#include <string>
#include <vector>

namespace zephyr {
int32_t split_string(const std::string &s, char delim, std::vector<std::string> *v);
std::string join_string(const std::vector<std::string> &parts, const std::string &separator);
std::string& ltrim(const std::string& chars, std::string& str);
std::string& rtrim(const std::string& chars, std::string& str);
std::string& trim(const std::string& chars, std::string& str);
}  // namespace zephyr

#endif  // ZEPHYR_STRING_UTIL_H_
