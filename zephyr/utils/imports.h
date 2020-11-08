#ifndef ZEPHYR_ZEPHYR_COMMON_IMPORTS_H_
#define ZEPHYR_ZEPHYR_COMMON_IMPORTS_H_

#include "string_util.h"
#include <cstdio>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace zephyr {
using std::function;
using std::string;
using std::thread;

// for smart pointer
using std::shared_ptr;
using std::unique_ptr;

// for collections
using std::map;
using std::pair;
using std::unordered_map;
using std::unordered_set;
using std::vector;

// for lock
using std::condition_variable;
using std::lock_guard;
using std::mutex;
using std::unique_lock;

// for zephyr
using Server = std::string;
using Meta = std::unordered_map<std::string, std::string>;
using MonitorCallBack = function<void(const Server &)>;

using TimePoint = std::chrono::time_point<std::chrono::system_clock>;
using Duration = std::chrono::seconds;
} // namespace zephyr

#endif // ZEPHYR_ZEPHYR_COMMON_IMPORTS_H_
