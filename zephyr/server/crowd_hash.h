#ifndef ZEPHYRCLIENT_CROWD_HASH_H
#define ZEPHYRCLIENT_CROWD_HASH_H

#include <stdint.h>
#include <stddef.h>
#include <cassert>

#include "matrix/common/hash_func.h"

namespace zephyr {
namespace server {
// stand pair waste memory, we need more compact
#pragma pack(push)
#pragma pack(2)

template<class Sign, class T>
struct entry_t {
    entry_t(Sign s, T d) : first(s), second(d) {}

    entry_t(Sign s) : first(s), second() {}

    entry_t() : first(), second() {}

    Sign first;
    T second;
};

#pragma pack(pop)

template<class Key, class T, class HashFunc = murmurhash64 <Key>>
class crowd_hash_iterator {
public:
    typedef Key key_type;
    typedef T data_type;
    typedef entry_t<key_type, data_type> value_type;
    typedef crowd_hash_iterator<Key, T, HashFunc> iterator;

    crowd_hash_iterator(value_type *p) : it_(p) {}

    crowd_hash_iterator() {}

    const value_type &get_all() const { return *it_; }

    value_type &get_all() { return *it_; }

    void set_all(const value_type &value) { *it_ = value; }

    data_type &get() { return it_->second; }

    const data_type &get() const { return it_->second; }

    void set(const data_type &data) { it_->second = data; }

    // Comparison.
    bool operator==(const iterator &it) const {
        return (it_ == it.it_);
    }

    bool operator!=(const iterator &it) const {
        return (it_ != it.it_);
    }

private:
    value_type *it_;
};

/*
*  we need a special key zero for empty
*/
template<class Key, class T, class HashFunc = murmurhash64 <Key>>
class crowd_hash {
public:
    typedef Key key_type;
    typedef T data_type;

    typedef entry_t<key_type, data_type> value_type;
    typedef crowd_hash_iterator<Key, T, HashFunc> iterator;

    // Constructor
    explicit crowd_hash(size_t num_buckets, size_t max_probes,
                        const key_type &empty_key, size_t occupyncy_pct = 97)
            : num_elements_(0),
              num_buckets_(num_buckets),
              max_probes_(max_probes),
              empty_key_(empty_key) {
        max_elements_ = static_cast<size_t>(num_buckets_ * occupyncy_pct / 100);
        assert(max_elements_ > 0);
        table_ = static_cast<value_type *>(malloc(num_buckets * sizeof(value_type)));
        if (table_ == NULL) {
            assert(1 && "error alloc memory");
            exit(-1);
        }
        value_type empty_value(empty_key_);
        std::uninitialized_fill(table_, table_ + num_buckets, empty_value);
    }

    explicit crowd_hash(value_type *table, size_t num_elements,
                        size_t num_buckets, size_t max_probes,
                        const key_type &empty_key, size_t occupyncy_pct = 97,
                        bool create = false)
            : own_table_(false), table_(table), num_elements_(num_elements),
              num_buckets_(num_buckets),
              max_probes_(max_probes),
              empty_key_(empty_key) {
        assert(table);
        max_elements_ = static_cast<size_t>(num_buckets_ * occupyncy_pct / 100);
        assert(max_elements_ > 0);
        if (create) {
            value_type empty_value(empty_key_);
            std::uninitialized_fill(table_, table_ + num_buckets, empty_value);
        }
    }

    ~crowd_hash() {
        if (own_table_) {
            free(table_);
        }
        table_ = NULL;
    }

    // Iterator
    iterator begin() { return table_; }

    iterator end() { return table_ + num_buckets_; }

    iterator find_key(const key_type &key, const size_t probe_num) {
        return find(hash(key), probe_num);
    }

    iterator find(const uint64_t hash_value, const size_t probe_num) {
        // 1 + 4 + 9 + 16 + 25 + ... + n^2 == n(n + 1)(2n + 1) / 6
        size_t pos = probe_num * (probe_num + 1) * (2 * probe_num + 1) / 6;
        pos = (pos + hash_value) & (bucket_count() - 1);
        return iterator(table_ + pos);
    }

    // Size
    size_t size() const { return num_elements_; }

    bool empty() const { return size() == 0; }

    bool full() const { return size() >= max_elements_; }

    size_t bucket_count() const { return num_buckets_; }

    // Insert
    // @ret true, if success and pnum is set to number of probe times
    //            pit is set to the iterator
    // @ret falue, if fail
    //             pnum set 0, if exist and pit is set to the iterator
    //             pnum set max_probes, if probes times reach the value
    bool insert_key(const key_type &key, const data_type &data, size_t *pnum, iterator *pit) {
        return insert(hash(key), key, data, pnum, pit);
    }

    bool
    insert(const uint64_t hash_value, const key_type &key, const data_type &data, size_t *pnum, iterator *pit) {
        size_t num_probes = 0;
        const size_t bucket_count_minus_one = bucket_count() - 1;
        size_t bucknum = hash_value & bucket_count_minus_one;

        while (true) {
            if (table_[bucknum].first == empty_key_)   // find a empty place to insert
            {
                break;
            }
            // do not check, if there is a exist value.
            if (++num_probes >= max_probes_) {
                *pnum = num_probes;
                return false;
            }
            bucknum = (bucknum + num_probes * num_probes) & bucket_count_minus_one;
        }
        // we can insert new value now
        table_[bucknum].first = key;
        table_[bucknum].second = data;

        // increase the elements number
        num_elements_++;

        // set the return value
        *pnum = num_probes;
        *pit = iterator(table_ + bucknum);
        return true;
    }

private:
    uint64_t hash(const key_type &key) const {
        return hash_func_.operator()(key);
    }

private:
    // Actual data
    bool own_table_ = true;
    value_type *table_;
    size_t num_elements_;
    size_t max_elements_;
    size_t num_buckets_;
    size_t max_probes_;

    // special key
    key_type empty_key_;

    // function objects
    HashFunc hash_func_;
};

}
}
#endif //ZEPHYRCLIENT_CROWD_HASH_H
