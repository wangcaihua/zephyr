#ifndef ZEPHYR_STATUS_H_
#define ZEPHYR_STATUS_H_

#include "zephyr/utils/imports.h"

namespace zephyr {
namespace common {

enum StatusCode {
  OK = 0,
  INVALID_ARGUMENT = 1,
  PROTO_ERROR = 2,
  RPC_ERROR = 3
};

class Status {
public:
  Status() : code_(StatusCode::OK) {}

  Status(StatusCode code, string message)
      : code_(code), message_(move(message)) {}

  static const Status &OK;

  bool ok() const { return code_ == StatusCode::OK; }

  string message() const { return message_; }

private:
  StatusCode code_;
  string message_;
};

using DoneCallBack = function<void(const Status &)>;
} // namespace common
} // namespace zephyr

#endif // ZEPHYR_STATUS_H_
