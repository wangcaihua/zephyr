/**
* @author qiaomu(qiaomu@bytedance.com)
* @date 03/30/2015 01:56:10 PM
* @brief
*
**/

#include "sliding_hash_filter.h"
#include <iostream>

namespace zephyr {
namespace server {
SlidingHashFilter::SlidingHashFilter(size_t capacity, int split_num) : head_(0), head_increment_(0),
                                                                       failure_count_(0) {
    capacity_ = capacity;
    if (capacity_ < 300)
        capacity_ = 300;
    if (split_num < 5)
        split_num = 5;
    filters_.resize(split_num);
    max_backward_step_ = split_num - max_forward_step_;
    // max_forward_step_ - 1 blocks are kept empty for looing forward
    size_t split_capacity = get_split_capacity(capacity_, split_num);
    for (auto &filter : filters_) {
        filter.reset(new HashFilter<uint16_t>(split_capacity, 1.2));
    }
}

SlidingHashFilter::SlidingHashFilter(const SlidingHashFilter &other) :
        max_backward_step_(other.max_backward_step_), filters_(other.filters_.size()),
        head_(other.head_), head_increment_(other.head_increment_),
        failure_count_(other.failure_count_) {
    capacity_ = other.capacity_;
    if (&other == this) {
        return;
    }
    for (size_t i = 0; i != filters_.size(); ++i) {
        filters_[i].reset(other.filters_[i]->clone());
    }
}

uint32_t SlidingHashFilter::add(FID fid, uint32_t count) {
    uint32_t old_count = 0;

    // Look forward to find current value
    HashFilterIterator <uint16_t> curr_iter = bidirectional_find(head_, max_forward_step_, fid, false,
                                                                 std::bind(&SlidingHashFilter::next, this,
                                                                           std::placeholders::_1));
    if (curr_iter.valid()) {
        if (!curr_iter.empty()) {
            return curr_iter.add(count);
        }
    } else {
        failure_count_ += 1;
        return HashFilter<uint16_t>::max_count();
    }

    // Look backward to find old value
    HashFilterIterator <uint16_t> old_iter = bidirectional_find(prev(head_),
                                                                std::min(head_increment_, max_backward_step_),
                                                                fid, true,
                                                                std::bind(&SlidingHashFilter::prev, this,
                                                                          std::placeholders::_1));
    if (old_iter.valid()) {
        old_count = old_iter.get();
        curr_iter.add(old_count + count);
    } else {
        curr_iter.add(count);
    }
    if (filters_[head_]->full()) {
        parameter_server::MetricCollector::get_instance()->emit_counter("filter_switch", 1, "model=" + name_);
        matrix::Timer t;
        head_ = next(head_);
        head_increment_ += 1;
        filters_[(head_ + max_forward_step_ - 1) % filters_.size()]->async_clear();
        parameter_server::MetricCollector::get_instance()->emit_timer("filter_switch_tm", t.elapsed(),
                                                                      "model=" + name_);
    }
    return old_count;
}

uint32_t SlidingHashFilter::get(FID fid) const {
    // Look forward to find current value
    HashFilterIterator <uint16_t> curr_iter = bidirectional_find(head_, max_forward_step_, fid, false,
                                                                 std::bind(&SlidingHashFilter::next, this,
                                                                           std::placeholders::_1));
    if (curr_iter.valid()) {
        if (!curr_iter.empty()) {
            return curr_iter.get();
        }
    } else {
        return HashFilter<uint16_t>::max_count();
    }

    // Look backward to find old value
    HashFilterIterator <uint16_t> iter = bidirectional_find(prev(head_),
                                                            std::min(head_increment_, max_backward_step_), fid,
                                                            true,
                                                            std::bind(&SlidingHashFilter::prev, this,
                                                                      std::placeholders::_1));
    if (iter.valid()) {
        return iter.get();
    } else {
        return 0;
    }
}

HashFilterIterator <uint16_t> SlidingHashFilter::bidirectional_find(size_t begin,
                                                                    int max_look,
                                                                    FID fid,
                                                                    bool exhaust,
                                                                    std::function<size_t(size_t)> go) const {
    size_t index = begin;
    for (int i = 0; i != max_look; ++i) {
        HashFilterIterator <uint16_t> iter = filters_[index]->find(fid, MAX_STEP);
        // Looking forward only needs a valid position
        // Looking backward needs a non-empty position
        if (iter.valid() && (!exhaust || (exhaust && !iter.empty())))
            return iter;
        index = go(index);
    }
    return HashFilterIterator<uint16_t>();
}

size_t SlidingHashFilter::estimated_total_element() const {
    size_t result = 0;
    for (auto &filter : filters_) {
        result += filter->estimated_total_element();
    }
    return result;
}

bool SlidingHashFilter::dump(
        const std::string &path, const std::string &model_name,
        int64_t rate_limit, const uint64_t operation_token) const {
    for (size_t i = 0, index = head_; i != filters_.size(); ++i, index = prev(index)) {
        if (!matrix::exec_with_retry(
                std::bind(
                        &Filter::dump, &(*filters_[index]), get_split_path(path, i),
                        model_name, rate_limit, operation_token))) {
            return false;
        }
    }
    return true;
}

bool SlidingHashFilter::load(
        const std::string &path, const std::string &model_name,
        int64_t rate_limit, const uint64_t operation_token,
        const int dump_version) {
    head_ = 0;
    head_increment_ = filters_.size();
    failure_count_ = 0;
    for (size_t i = 0, index = head_; i != filters_.size(); ++i, index = prev(index)) {
        if (!matrix::exec_with_retry(
                std::bind(
                        &Filter::load, &(*filters_[index]), get_split_path(path, i),
                        model_name, rate_limit, operation_token, dump_version))) {
            return false;
        }
    }
    return true;
}

SlidingHashFilter *SlidingHashFilter::clone() const {
    return new SlidingHashFilter(*this);
}

bool SlidingHashFilter::operator==(const SlidingHashFilter &other) const {
    if (!(max_forward_step_ == other.max_forward_step_ && head_ == other.head_ &&
          head_increment_ % filters_.size() ==
          other.head_increment_ % filters_.size() &&
          capacity_ == other.capacity_ &&
          filters_.size() == other.filters_.size())) {
        return false;
    }
    for (size_t i = 0; i != filters_.size(); ++i) {
        if (!(*filters_[i] == *other.filters_[i])) {
            return false;
        }
    }
    return true;
}
}
}