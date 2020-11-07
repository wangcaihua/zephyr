#pragma once

#include <atomic>
#include <thread>
#include <vector>

#include "likely.h"
#include "matrix/common/fine_grid_lock.hpp"
#include "matrix/common/hash_func.h"
#include "matrix/common/types.h"
#include "sparsehash/dense_hash_set"

namespace zephyr {
namespace server {
#pragma pack(push)
#pragma pack(4)
template<typename Key>
struct hopscotch_entry_t {
    Key key;
    uint32_t hash;
    uint32_t hop_info;
};
#pragma pack(pop)

// thread safe hopscotch hash set (insert only)
// paper: http://people.csail.mit.edu/shanir/publications/disc2008_submission_98.pdf
template<typename Key>
class HopscotchHashSet {
public:
    explicit HopscotchHashSet(uint32_t capacity, uint32_t concurrency_level);

    // thread safe insert, return number keys cleared
    size_t insert(Key key);

    // return all keys, not thread safe
    std::vector<Key> get_and_clear();

    size_t size() const { return num_elements_.load(std::memory_order_relaxed); }

private:
    uint32_t hash_func(Key key) { return hash_func_(key) | 3; }

    void find_closer_free_bucket(const neo::folly_spinlock_wrapper *lock,
                                 int *free_bucket, int *free_dist);

    void do_init();

    // clear the hash table, not thread safe
    void do_clear();

private:
    static constexpr uint32_t kHopscotchHashInsertRange = 4096;
    static constexpr uint32_t kHopscotchHashHopRange = 32;
    static constexpr uint32_t kHopscotchHashEmpty = 0;
    static constexpr uint32_t kHopscotchHashBusy = 1;
    static constexpr Key kEmptyKey = Key(-1);

    neo::murmurhash64 <Key> hash_func_;

    ::google::dense_hash_set <Key> extra_;  // for those keys not insert into table
    neo::folly_spinlock_wrapper extra_lock_;
    neo::folly_spinlock_wrapper clear_lock_;
    neo::folly_spinlock_wrapper init_lock_;
    std::vector<hopscotch_entry_t<Key>> table_;
    std::vector<neo::folly_spinlock_wrapper> locks_;
    uint32_t lock_mask_;
    uint32_t bucket_mask_;
    std::atomic_int running_threads_;  // number of thread doing insertion
    std::atomic_int num_elements_;     // total number of elements
    uint32_t capacity_;
    bool init_;
};

}
}