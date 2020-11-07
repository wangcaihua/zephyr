#ifndef ZEPHYR_SLIDING_HASH_FILTER_H
#define ZEPHYR_SLIDING_HASH_FILTER_H

#include <functional>
#include "filter.h"
#include "hash_filter.h"


namespace zephyr {
namespace server {

class SlidingHashFilter : public Filter {
public:
    SlidingHashFilter(size_t capacity, int split_num);

    SlidingHashFilter(const SlidingHashFilter &other);

    uint32_t add(FID fid, uint32_t count);

    uint32_t get(FID fid) const;

    uint32_t size_mb() const {
        return filters_[0]->size_mb() * filters_.size();
    }

    static size_t get_split_capacity(size_t capacity, int split_num) {
        return capacity / (split_num - max_forward_step_ + 1);
    }

    static uint32_t size_byte(size_t capacity, int split_num) {
        return HashFilter<uint16_t>::size_byte(
                get_split_capacity(capacity, split_num), 1.2) * split_num;
    }

    size_t estimated_total_element() const;

    size_t failure_count() const {
        return failure_count_;
    }

    bool dump(
            const std::string &path, const std::string &model_name,
            int64_t rate_limit, const uint64_t operation_token) const;

    bool load(
            const std::string &path, const std::string &model_name,
            int64_t rate_limit, const uint64_t operation_token,
            const int dump_version);

    SlidingHashFilter &operator=(SlidingHashFilter const &) = delete;

    SlidingHashFilter *clone() const;

    bool operator==(SlidingHashFilter const &) const;

private:
    size_t prev(size_t index) const {
        if (index == 0)
            return filters_.size() - 1;
        return index - 1;
    }

    size_t next(size_t index) const {
        if (index == filters_.size() - 1)
            return 0;
        return index + 1;
    }

    HashFilterIterator <uint16_t> bidirectional_find(size_t begin,
                                                     int max_look,
                                                     FID fid,
                                                     bool exhaust,
                                                     std::function<size_t(size_t)> go) const;

    static std::string get_split_path(const std::string &path, size_t index) {
        return path + ".filter_part" + boost::lexical_cast<std::string>(index);
    }

    constexpr static int max_forward_step_ = 2;
    int max_backward_step_;
    constexpr static int MAX_STEP = 16;
    std::vector<std::unique_ptr<HashFilter < uint16_t>>>
    filters_;
    size_t head_;
    int head_increment_;
    size_t failure_count_;
};

}
}
#endif /* ZEPHYR_SLIDING_HASH_FILTER_H */
