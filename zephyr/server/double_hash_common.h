#ifndef ZEPHYR_DOUBLE_HASH_COMMON_H
#define ZEPHYR_DOUBLE_HASH_COMMON_H

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#endif //ZEPHYR_DOUBLE_HASH_COMMON_H
