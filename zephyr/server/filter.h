#ifndef FILTER_H
#define FILTER_H
#include "matrix/common/types.h"

namespace zephyr {
namespace server {
class Filter {
public:
    Filter() : capacity_(0) {}

    virtual uint32_t add(FID fid, uint32_t count) = 0;

    virtual uint32_t get(FID fid) const = 0;

    virtual uint32_t size_mb() const = 0;

    virtual size_t estimated_total_element() const = 0;

    virtual size_t failure_count() const = 0;

    virtual bool dump(
            const std::string &path, const std::string &model_name,
            int64_t rate_limit, const uint64_t operation_token) const = 0;

    virtual bool load(
            const std::string &path, const std::string &model_name,
            int64_t rate_limit, const uint64_t operation_token,
            const int dump_version) = 0;

    virtual size_t capacity() const {
        return capacity_;
    }

    virtual bool exceed_limit() const {
        return false;
    }

    virtual void set_name(const std::string &name) {
        name_ = name;
    }

    virtual Filter *clone() const = 0;

    virtual ~Filter() {}

    constexpr static unsigned char count_bit = 4;

    constexpr static uint32_t max_count() {
        return (1 << count_bit) - 1;
    }

protected:
    size_t capacity_;
    std::string name_;
};

}
#endif /* FILTER_H */
}