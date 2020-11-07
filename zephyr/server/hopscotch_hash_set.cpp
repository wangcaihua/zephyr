#include "hopscotch_hash_set.h"

namespace zephyr {
namespace server {
    using neo::FID;

    inline static uint32_t next_power_of_two(uint32_t n) {
        --n;
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        return n + 1;
    }

    inline static int first_lsb_bit_index(uint32_t x) {
        return __builtin_ffs(x) - 1;
    }

    template<typename Key>
    HopscotchHashSet<Key>::HopscotchHashSet(uint32_t capacity,
                                            uint32_t concurrency_level)
            : capacity_(capacity), init_(false) {
        lock_mask_ = next_power_of_two(concurrency_level) - 1;
        bucket_mask_ = next_power_of_two(capacity * 1.2) - 1;
        init_lock_.init();
    }

    template<typename Key>
    void HopscotchHashSet<Key>::do_init() {
        table_.resize(bucket_mask_ + kHopscotchHashInsertRange + 1);
        locks_.resize(lock_mask_ + 1);
        for (size_t i = 0; i <= lock_mask_; ++i) {
            locks_[i].init();
        }
        extra_lock_.init();
        clear_lock_.init();
        extra_.set_empty_key(-1);
        num_elements_.store(0, std::memory_order_seq_cst);
        running_threads_.store(0, std::memory_order_seq_cst);
        do_clear();
    }

    template<typename Key>
    void HopscotchHashSet<Key>::find_closer_free_bucket(
            const neo::folly_spinlock_wrapper *lock, int *free_bucket, int *free_dist) {
        int move_bucket = *free_bucket - (kHopscotchHashHopRange - 1);
        int move_free_dist;
        for (move_free_dist = kHopscotchHashHopRange - 1; move_free_dist > 0;
             --move_free_dist) {
            auto new_lock = &locks_[move_bucket & lock_mask_];
            uint32_t start_hop_info = table_[move_bucket].hop_info;
            int move_new_free_dist = !start_hop_info ? kHopscotchHashHopRange
                                                     : __builtin_ctz(start_hop_info);
            if (move_new_free_dist < move_free_dist) {
                if (new_lock != lock) {
                    new_lock->lock();
                }
                if (start_hop_info == table_[move_bucket].hop_info) {
                    // new_free_bucket -> free_bucket and empty new_free_bucket
                    int new_free_bucket = move_bucket + move_new_free_dist;
                    table_[*free_bucket].key = table_[new_free_bucket].key;
                    table_[*free_bucket].hash = table_[new_free_bucket].hash;
                    table_[move_bucket].hop_info |= 1u << move_free_dist;
                    table_[move_bucket].hop_info &= ~(1u << move_new_free_dist);

                    *free_bucket = new_free_bucket;
                    *free_dist -= move_free_dist - move_new_free_dist;
                    if (new_lock != lock) {
                        new_lock->unlock();
                    }
                    return;
                }
                if (new_lock != lock) {
                    new_lock->unlock();
                }
            }
            ++move_bucket;
        }
        *free_bucket = -1;
        *free_dist = 0;
    }

    template<typename Key>
    size_t HopscotchHashSet<Key>::insert(Key key) {
        // we do lazy init here to save memory
        if (!init_) {
            init_lock_.lock();
            if (!init_) do_init();
            init_ = true;
            init_lock_.unlock();
        }
        size_t dropped_keys = 0;
        if (unlikely(size() > capacity_)) {
            clear_lock_.lock();
            if (likely(size() > capacity_)) {
                for (int i = 0; i < locks_.size(); ++i) locks_[i].lock();
                dropped_keys = size();
                this->do_clear();
                for (int i = 0; i < locks_.size(); ++i) locks_[i].unlock();
            }
            clear_lock_.unlock();
        }
        uint32_t hash = hash_func(key);
        auto lock = &locks_[hash & lock_mask_];
        lock->lock();
        int bucket = hash & bucket_mask_;
        uint32_t hop_info = table_[bucket].hop_info;
        // check if already exists
        while (0 != hop_info) {
            int i = first_lsb_bit_index(hop_info);
            int current = bucket + i;
            if (key == table_[current].key) {
                lock->unlock();
                return dropped_keys;
            }
            hop_info &= ~(1U << i);
        }
        // looking for free bucket
        int free_bucket = bucket, free_dist = 0;
        for (; free_dist < kHopscotchHashInsertRange; ++free_dist, ++free_bucket) {
            if (kHopscotchHashEmpty == table_[free_bucket].hash &&
                kHopscotchHashEmpty ==
                __sync_val_compare_and_swap(&table_[free_bucket].hash,
                                            kHopscotchHashEmpty, hash)) {
                break;
            }
        }

        // insert the new key
        num_elements_.fetch_add(1, std::memory_order_relaxed);
        if (free_dist < kHopscotchHashInsertRange) {
            do {
                if (free_dist < kHopscotchHashHopRange) {
                    table_[free_bucket].key = key;
                    table_[free_bucket].hash = hash;
                    table_[bucket].hop_info |= 1u << free_dist;
                    lock->unlock();
                    return dropped_keys;
                }
                find_closer_free_bucket(lock, &free_bucket, &free_dist);
            } while (-1 != free_bucket);
        } else {
            // insert failed, insert into extra_ map
            extra_lock_.lock();
            extra_.insert(key);
            extra_lock_.unlock();
        }
        lock->unlock();
        return dropped_keys;
    }

    template<typename Key>
    std::vector<Key> HopscotchHashSet<Key>::get_and_clear() {
        if (!init_) return {};
        clear_lock_.lock();
        for (int i = 0; i < locks_.size(); ++i) locks_[i].lock();
        std::vector<Key> results(size());
        size_t index = 0;
        for (auto &&entry : table_) {
            if (entry.hash) {
                results[index++] = entry.key;
            }
            entry.hash = 0;
            entry.key = kEmptyKey;
            entry.hop_info = 0;
        }
        for (auto &&key : extra_) {
            results[index++] = key;
        }
        extra_.clear();
        num_elements_.store(0, std::memory_order_seq_cst);
        for (int i = 0; i < locks_.size(); ++i) locks_[i].unlock();
        clear_lock_.unlock();
        return results;
    }

    template<typename Key>
    void HopscotchHashSet<Key>::do_clear() {
        for (size_t i = 0; i < table_.size(); ++i) {
            table_[i].hash = 0;
            table_[i].key = kEmptyKey;
            table_[i].hop_info = 0;
        }
        num_elements_.store(0, std::memory_order_seq_cst);
        extra_.clear();
    }

    template
    class HopscotchHashSet<FID>;

}
}