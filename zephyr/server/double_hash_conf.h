#ifndef ZEPHYR_DOUBLE_HASH_CONF_H
#define ZEPHYR_DOUBLE_HASH_CONF_H

#include <string>

namespace zephyr {
namespace server {

class double_hash_conf
{
public:
// total blocks: 2^bit_blocks - 1
size_t bit_blocks = 14;

// max probe times: 2^bit_probes
size_t bit_probes = 5;

// number of bits to store layer1 sign
size_t bit_sign = 5;

// How full we let the table get before we resize, by default.
size_t first_level_occupyncy_pct = 75;
size_t second_level_occupyncy_pct = 97;

// starting buckets size, MUST BE the power of 2
size_t starting_buckets = 32;

// file name for disk store
std::string file_name = "extra.db";

// asynchronized file buffers
size_t num_buffer = 4;
size_t buffer_size = (128UL * 1024);
};

static double_hash_conf g_default_conf;

}
}
#endif //ZEPHYRCLIENT_DOUBLE_HASH_CONF_H
