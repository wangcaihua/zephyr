#ifndef ZEPHYR_DOUBLEHASH_H_
#define ZEPHYR_DOUBLEHASH_H_

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <functional>
#include <utility>
#include <limits>

#include "matrix/common/fine_grid_lock.hpp"
#include "matrix/common/hash_func.h"
#include "idl/parameter_server/ParameterServer.h"
#include "crowd_hash.h"
#include "async_file.h"
#include "double_hash_conf.h"
#include "double_hash_common.h"
#include "parameter_server/common/error_code.h"

#include <sys/mman.h>
#include <sys/file.h>
#include <sys/param.h>

#ifdef __aarch64__
#include <glog/logging.h>
#endif
#include <gflags/gflags.h>


namespace zephyr {
namespace server {
// for first level hash table
template<class Item>
class table_t {
public:
    typedef Item *table_type;
    typedef table_type iterator;

    table_t() : table_(NULL), size_(0) {}

    ~table_t() { if (own_table_) free(table_); }

    size_t size() const { return size_; }

    iterator begin() { return &table_[0]; }

    iterator end() { return &table_[size() - 1]; }

    iterator get(size_t n) { return &table_[n]; }

    void init(size_t size, table_type table = nullptr, bool create = false) {
        if (size == 0) {
            table_ = NULL;
            return;
        }
        size_ = size;
        if (table) {
            table_ = table;
            own_table_ = false;
            if (create) {
                memset(table_, 0, sizeof(Item) * size);
            }
        } else {
            table_ = static_cast<table_type>(malloc(sizeof(Item) * size));
            if (table_ == NULL) {
                assert(0 && "error alloc memory");
                exit(-1);
            }
            own_table_ = true;
            memset(table_, 0, sizeof(Item) * size);
        }
    }

    void swap(table_t &t) {
        std::swap(table_, t.table_);
        std::swap(size_, t.size_);
        std::swap(own_table_, t.own_table_);
    }

private:
    bool own_table_ = true;
    table_type table_;
    size_t size_;
};

template<class Key, class T, class Extra, class HashFunc, class SignFunc>
class double_hash;

// for file iterator.
// read operation is synchronized.
// write operation is asynchronized. more complicated.
template<class Key, class T, class Extra>
struct obj_t {
    Key key;
    Extra extra;
    T data;
};

#pragma pack(push)
#pragma pack(1)
template<class Key, class Extra>
struct key_extra_t {
    Key key;
    Extra extra;
};
#pragma pack(pop)

// in mem iterator for scanning
template<class Key, class T, class Extra, class HashFunc, class SignFunc>
class double_hash_file_read_iterator {
public:
    typedef Key key_type;
    typedef T data_type;
    typedef Extra extra_type;
    typedef obj_t<key_type, T, extra_type> obj_type;
    typedef double_hash<Key, T, Extra, HashFunc, SignFunc> double_hash_type;
    typedef key_extra_t<key_type, extra_type> key_extra_type;

    explicit double_hash_file_read_iterator(
            double_hash_type *dh, uint32_t buffer_size, size_t offset = 0,
            size_t total_elements = std::numeric_limits<size_t>::max())
            : dh_(dh),
              begin_(0),
              end_(0),
              buffer_size_(buffer_size),
              offset_(offset),
              count_(0),
              total_elements_(total_elements) {
        fs_ = std::shared_ptr<std::ifstream>(
                new std::ifstream(dh->conf_.file_name + ".aof"));

        LOG(INFO) << "double_hash_file_read_iterator openning file["
                  << dh->conf_.file_name + ".aof"
                  << "]";

        if (!fs_->is_open()) {
            LOG(ERROR) << "double_hash_file_read_iterator openning file["
                       << dh->conf_.file_name + ".aof"
                       << "] failed; reason[" << strerror(errno) << "]" << std::endl;
            assert(0 && "error open file, maybe cwd is not writable");
            exit(-1);
        }

        fs_->seekg(offset_ * sizeof(key_extra_type), fs_->beg);

        buffer_ = std::shared_ptr<key_extra_type>(
                reinterpret_cast<key_extra_type *>(
                        malloc(sizeof(key_extra_type) * buffer_size_)),
                std::free);
    }

    double_hash_file_read_iterator(
            double_hash_file_read_iterator &&buffered_file_read_it)
            : dh_(nullptr),
              fs_(nullptr),
              buffer_(nullptr),
              begin_(0),
              end_(0),
              buffer_size_(0),
              offset_(0),
              count_(0),
              total_elements_(std::numeric_limits<size_t>::max()) {
        std::swap(dh_, buffered_file_read_it.dh_);
        std::swap(fs_, buffered_file_read_it.fs_);
        std::swap(obj_, buffered_file_read_it.obj_);
        std::swap(buffer_, buffered_file_read_it.buffer_);
        std::swap(begin_, buffered_file_read_it.begin_);
        std::swap(end_, buffered_file_read_it.end_);
        std::swap(offset_, buffered_file_read_it.offset_);
        std::swap(count_, buffered_file_read_it.count_);
        std::swap(total_elements_, buffered_file_read_it.total_elements_);
    }

    double_hash_file_read_iterator(const double_hash_file_read_iterator &) =
    delete;

    double_hash_file_read_iterator &operator=(double_hash_file_read_iterator &) =
    delete;

    bool read_key() const {
        while (count_ < total_elements_) {
            if (begin_ != end_) {
                obj_.key = buffer_.get()[begin_].key;
                obj_.extra = buffer_.get()[begin_].extra;
                begin_++;
                return true;
            } else {
                if (fs_->eof()) return false;
                if (fs_->fail()) {
                    LOG(ERROR) << "IO Error for " << dh_->conf_.file_name << ".aof"
                               << std::endl;
                    return false;
                }
                fs_->read(reinterpret_cast<char *>(buffer_.get()),
                          sizeof(key_extra_type) * buffer_size_);
                begin_ = 0;
                end_ = fs_->gcount() / sizeof(key_extra_type);
            }
        }
        return false;
    }

    bool read() {
        if (!read_key()) {
            return false;
        }
        count_++;
        return true;
    }

    const obj_type &operator*() const { return obj_; }

    const obj_type *operator->() const { return &(operator*()); }

private:
    double_hash_type *dh_;
    std::shared_ptr<std::ifstream> fs_;
    mutable obj_type obj_;
    mutable std::shared_ptr<key_extra_type> buffer_;
    mutable size_t begin_;
    mutable size_t end_;
    size_t buffer_size_;
    size_t offset_;
    size_t count_;
    size_t total_elements_;
};

// file iterator for writing
class double_hash_file_write_iterator {
public:
    explicit double_hash_file_write_iterator(const std::string file_name,
                                             size_t num_buffer,
                                             size_t buffer_size,
                                             int flags = O_TRUNC | O_CREAT)
            : fs_(num_buffer, buffer_size) {
        std::cerr << "double_hash_file_write_iterator openning file[" << file_name
                  << "]" << std::endl;
        if (!fs_.open(file_name, flags)) {
            throw std::system_error(errno, std::system_category(),
                                    "failed to open file: " + file_name);
        }
        std::cerr << "double_hash_file_write_iterator openning file[" << file_name
                  << "] success" << std::endl;
    }

    ~double_hash_file_write_iterator() {}

    double_hash_file_write_iterator(double_hash_file_write_iterator &) = delete;

    double_hash_file_write_iterator &operator=(double_hash_file_write_iterator &) =
    delete;

    template<class key_type, class extra_type>
    void write(const key_type &key, const extra_type &extra, int thread_index = -1) {
        key_extra_t<key_type, extra_type> elem{key, extra};
        fs_.async_write(elem, thread_index);
    }

    void flush() { fs_.flush(); }

private:
    async_stream fs_;
};

/*
#pragma pack(push)
#pragma pack(4)
struct spinned_item {
uint8_t values[3];
folly_spinlock_wrapper lock;
// total blocks: 2^bit_blocks - 1
static constexpr size_t kBitBlocks = 14;
static constexpr size_t kMaskBlocks = (1 << kBitBlocks) - 1;
// one slot is used for empty value.
static constexpr size_t kMaxBlocks = (1 << kBitBlocks) - 1;
// max probe times: 2^bit_probes
static constexpr size_t kBitProbes = 5;
static constexpr size_t kMaskProbes = (1 << kBitProbes) - 1;
// number of bits to store layer1 sign
static constexpr size_t kBitSign = 5;
static constexpr size_t kMaskSign = (1 << kBitSign) - 1;
static_assert(kBitProbes + kBitProbes + kBitSign <= 24,
        "no more than 24 bits");

// item(for table) operation
// block 0 means 0, skip it.
uint32_t block() const {
const uint32_t *value = reinterpret_cast<const uint32_t *>(this);
return (*value & kMaskBlocks) - 1;
}
uint32_t probe() const {
const uint32_t *value = reinterpret_cast<const uint32_t *>(this);
return *value >> kBitBlocks & kMaskProbes;
}
uint32_t sign() const {
const uint32_t *value = reinterpret_cast<const uint32_t *>(this);
return *value >> (kBitBlocks + kBitProbes) & kMaskSign;
}
bool empty() const {
const uint32_t *value = reinterpret_cast<const uint32_t *>(this);
return (*value << 8) == 0;
}
void make_item(uint32_t block, uint32_t probe, uint32_t sign) {
uint32_t *value = reinterpret_cast<uint32_t *>(this);
*value = (*value >> 24 << 24) | ((block + 1) | (probe << kBitBlocks) |
                             (sign << (kBitBlocks + kBitProbes)));
}
};
#pragma pack(pop)
*/

#pragma pack(push)
#pragma pack(1)

struct spinned_item {
    uint8_t values[2];
    onebit_spinlock_wrapper lock;
    // max probe times: 2^bit_probes
    static constexpr size_t kBitProbes = 5;
    static constexpr size_t kMaskProbes = (1 << kBitProbes) - 1;
    // number of bits to store layer1 sign
    static constexpr size_t kBitSign = 5;
    static constexpr size_t kMaskSign = (1 << kBitSign) - 1;
    // total blocks: 2^bit_blocks - 1
    static constexpr size_t kBitBlocks = 13;
    static constexpr size_t kMaskBlocks = (1 << kBitBlocks) - 1;
    // one slot is used for empty value.
    static constexpr size_t kMaxBlocks = (1 << kBitBlocks) - 1;
    static_assert(kBitProbes + kBitProbes + kBitSign < 24,
                  "less than 24 bits");
    static_assert(kBitProbes + kBitSign < 16,
                  "less than 2 bytes");

    // item(for table) operation
    uint32_t probe() const {
        const uint16_t *value = reinterpret_cast<const uint16_t *>(this);
        return *value & kMaskProbes;
    }

    uint32_t sign() const {
        const uint16_t *value = reinterpret_cast<const uint16_t *>(this);
        return *value >> kBitProbes & kMaskSign;
    }

    // block 0 means 0, skip it.
    uint32_t block() const {
        const uint16_t *value = reinterpret_cast<const uint16_t *>(this);
        return (*value >> (kBitProbes + kBitSign)) + (lock.value() << (16 - kBitProbes - kBitSign)) - 1;
    }

    bool empty() const {
        const uint16_t *value = reinterpret_cast<const uint16_t *>(this);
        return *value == 0 && lock.value() == 0;
    }

    void make_item(uint32_t block, uint32_t probe, uint32_t sign) {
        uint16_t *value = reinterpret_cast<uint16_t *>(this);
        uint32_t cval = (((block + 1) << (kBitProbes + kBitSign)) | probe | (sign << kBitProbes));
        *value = cval & 0xFFFF;
        lock.set(cval >> 16);
    }
};

#pragma pack(pop)

const uint64_t kInvalidLockIndex = std::numeric_limits<uint64_t>::max();

template<typename table_t>
class double_hash_lock_holder {
public:
    double_hash_lock_holder(table_t *table)
            : table_(table), lock_index_(kInvalidLockIndex) {}

    ~double_hash_lock_holder() {
        if ((!!table_) && likely(lock_index_ != kInvalidLockIndex)) {
            table_->unlock(lock_index_);
        }
    }

    uint64_t *lock_index_ptr() { return &lock_index_; }

private:
    table_t *table_;
    uint64_t lock_index_;
};

template<class Key, class T, class Extra = int32_t,
        class HashFunc = murmurhash64 <Key>,
        class SignFunc = simplehash32_0 <uint64_t>>
class double_hash {
public:
    typedef Key key_type;
    typedef T data_type;
    typedef Extra extra_type;
    typedef uint16_t item_type;
    typedef uint32_t sign_type;
    typedef entry_t <sign_type, data_type> value_type;

    typedef table_t<spinned_item> table_type;
    typedef typename table_type::iterator iterator;

    typedef crowd_hash <sign_type, data_type, HashFunc> block_t;
    typedef typename block_t::iterator accessor;

    typedef double_hash_file_read_iterator<Key, T, Extra, HashFunc, SignFunc> file_read_iterator;
    typedef double_hash_file_write_iterator file_write_iterator;
    typedef std::unique_ptr<void, std::function<void(void *)>> memory_holder_type;

    double_hash(std::function<bool(size_t)> memory_available_is_enough_checker) :
            num_elements_(0), mem_size_(0), crowd_hash_blocks_(NULL), key_missing_num_(0),
            memory_available_is_enough_checker_(memory_available_is_enough_checker) {
        layer2_lock_.init();
    }

    // throws iff 初始化失败
    void init(size_t, size_t num_elements = 0,
              const double_hash_conf &conf = g_default_conf, bool shm = false) {
        assert(crowd_hash_blocks_ == NULL && "Multiple initialization");
        conf_ = conf;
        file_it_.reset(new file_write_iterator(conf.file_name + ".aof",
                                               conf.num_buffer,
                                               conf.buffer_size));
        // initialization
        size_t num_buckets = min_buckets(num_elements, conf_.starting_buckets);
        num_threshold_ = size_t(num_buckets * (conf_.first_level_occupyncy_pct / 100.0));

        // alloc table
        if (!memory_available_is_enough_checker_(num_buckets * sizeof(spinned_item))) {
            neo::InvalidOperation io;
            io.err_code = parameter_server::ErrorCode::NOT_ENOUGH_MEMORY;
            io.why = "not enough memory available for table_ allocation while init";
            LOG(WARNING) << io.why;
            throw io;
        }
        if (shm) {
            table_holder_ = map_table(get_shm_basename() + ".tbl", num_buckets, true);
            table_.init(num_buckets, static_cast<table_type::table_type>(table_holder_.get()), true);
        } else {
            table_.init(num_buckets);
        }

        total_elements_limit_ = kMaxMachineMemory / sizeof(value_type) / 2;

        // prepare for blocks
        size_block_ = min_buckets(0, total_elements_limit_ / kMaxBlocks + 1);

        // alloc block
        if (shm) {
            auto block_name = get_shm_basename() + ".blk";
            block_fd_ = shm_open(block_name.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
            if (block_fd_ < 0) {
                throw std::system_error(errno, std::system_category(),
                                        "Failed to shm_open " + block_name);
            }
        }
        if (!memory_available_is_enough_checker_(kMaxBlocks * sizeof(block_t))) {
            neo::InvalidOperation io;
            io.err_code = parameter_server::ErrorCode::NOT_ENOUGH_MEMORY;
            io.why = "not enough memory available for croud_hash_blocks_ allocation while init";
            LOG(WARNING) << io.why;
            throw io;
        }
        crowd_hash_blocks_ = new block_t *[kMaxBlocks];
        memset(crowd_hash_blocks_, 0, kMaxBlocks * sizeof(block_t *));
        begin_block_ = 0;
        block_t *block = alloc_block(&begin_block_);
        assert(block != NULL);
        mem_size_ +=
                sizeof(table_type) + sizeof(spinned_item) * table_.size() + sizeof(block_t *) * kMaxBlocks + \
             conf.num_buffer * conf.buffer_size;
    }

    memory_holder_type map_table(
            std::string name, size_t num_buckets, bool create = false) {
        int fd = shm_open(name.c_str(), O_RDWR | (create ? (O_CREAT | O_TRUNC) : 0), 0644);
        if (fd < 0) {
            throw std::system_error(errno, std::system_category(),
                                    "Failed to open " + name);
        }
        if (flock(fd, LOCK_EX | LOCK_NB)) {
            close(fd);
            throw std::system_error(errno, std::system_category(),
                                    "Failed to lock " + name);
        }
        size_t size = roundup(num_buckets * sizeof(spinned_item),
                              sysconf(_SC_PAGESIZE));
        if (ftruncate(fd, size)) {
            close(fd);
            throw std::system_error(errno, std::system_category(),
                                    "Failed to ftruncate " + name);
        }
        void *ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                         MAP_SHARED | MAP_POPULATE, fd, 0);
        close(fd);
        if (ptr == MAP_FAILED) {
            throw std::system_error(errno, std::system_category(),
                                    "Failed to map " + name);
        }
        return memory_holder_type(ptr, [size](void *ptr) { munmap(ptr, size); });
    }

    void save() {
        auto name = get_shm_basename() + ".tbl";
        int fd = shm_open(name.c_str(), O_RDONLY, 0);
        if (fd < 0) {
            return;
        }
        close(fd);
        name = get_shm_basename() + ".blk";
        fd = shm_open(name.c_str(), O_RDONLY, 0);
        if (fd < 0) {
            return;
        }
        close(fd);
        name = get_shm_basename() + ".meta";
        fd = shm_open(name.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
        if (fd < 0) {
            throw std::system_error(errno, std::system_category(),
                                    "Failed to open " + name);
        }
        hash_table_meta meta;
        meta.size_block = size_block_;
        meta.num_buckets = bucket_count();
        meta.num_elements = size();
        size_t num_blocks = 0;
        while ((num_blocks < kMaxBlocks) && (!!crowd_hash_blocks_) && (crowd_hash_blocks_[num_blocks])) {
            ++num_blocks;
        }
        meta.num_blocks = num_blocks;
        meta.begin_block = begin_block_;
        if (write(fd, &meta, sizeof(meta)) != sizeof(meta)) {
            close(fd);
            throw std::system_error(errno, std::system_category(),
                                    "Failed to write " + name);
        }
        for (size_t i = 0; i < kMaxBlocks && !!crowd_hash_blocks_; ++i) {
            if (!crowd_hash_blocks_[i]) {
                break;
            }
            block_meta bmeta;
            bmeta.num_elements = crowd_hash_blocks_[i]->size();
            if (write(fd, &bmeta, sizeof(bmeta)) != sizeof(bmeta)) {
                close(fd);
                throw std::system_error(errno, std::system_category(),
                                        "Failed to write " + name);
            }
        }
        if (close(fd)) {
            throw std::system_error(errno, std::system_category(),
                                    "Failed to close " + name);
        }
    }

    static void unlink(const double_hash_conf &conf = g_default_conf) {
        std::string basename = get_shm_basename(conf);
        shm_unlink((basename + ".meta").c_str());
        shm_unlink((basename + ".tbl").c_str());
        shm_unlink((basename + ".blk").c_str());
        ::unlink((conf.file_name + ".aof").c_str());
    }

    void rename(const std::string &name) {
        if (conf_.file_name != name) {
            std::string dir = "/dev/shm";
            double_hash_conf new_conf;
            new_conf.file_name = name;
            if (block_fd_ >= 0) {
                if (::rename((dir + get_shm_basename() + ".tbl").c_str(),
                             (dir + get_shm_basename(new_conf) + ".tbl").c_str()) != 0) {
                    throw std::system_error(errno, std::system_category(),
                                            "Failed to rename to " + name);
                }
                if (::rename((dir + get_shm_basename() + ".blk").c_str(),
                             (dir + get_shm_basename(new_conf) + ".blk").c_str()) != 0) {
                    int error = errno;
                    if (::rename((dir + get_shm_basename(new_conf) + ".tbl").c_str(),
                                 (dir + get_shm_basename() + ".tbl").c_str()) != 0) {
                        abort();
                    }
                    throw std::system_error(error, std::system_category(),
                                            "Failed to rename to " + name);
                }
            }
            if (::rename((conf_.file_name + ".aof").c_str(), (name + ".aof").c_str()) != 0) {
                int error = errno;
                if (block_fd_ >= 0) {
                    if (::rename((dir + get_shm_basename(new_conf) + ".blk").c_str(),
                                 (dir + get_shm_basename() + ".blk").c_str()) != 0) {
                        abort();
                    }
                    if (::rename((dir + get_shm_basename(new_conf) + ".tbl").c_str(),
                                 (dir + get_shm_basename() + ".tbl").c_str()) != 0) {
                        abort();
                    }
                }
                throw std::system_error(error, std::system_category(),
                                        "Failed to rename to " + name);
            }
            conf_.file_name = name;
        }
    }

    static double_hash *open(
            size_t total_elements_limit,
            std::function<bool(size_t)> memory_available_is_enough_checker,
            const double_hash_conf &conf = g_default_conf, bool create = false,
            size_t num_elements = 0, bool use_shm = false) {
        double_hash *dh = new double_hash(memory_available_is_enough_checker);
        try {
            if (create) {
                dh->init(total_elements_limit, num_elements, conf, use_shm);
            } else {
                dh->restore(total_elements_limit, conf);
            }
            shm_unlink((dh->get_shm_basename() + ".meta").c_str());
        } catch (...) {
            delete dh;
            throw;
        }
        return dh;
    }

    void restore(size_t, const double_hash_conf &conf) {
        assert(crowd_hash_blocks_ == NULL && "Multiple initialization");
        conf_ = conf;
        file_it_.reset(new file_write_iterator(conf.file_name + ".aof",
                                               conf.num_buffer,
                                               conf.buffer_size,
                                               O_APPEND));
        auto name = get_shm_basename() + ".meta";
        int fd = shm_open(name.c_str(), O_RDONLY, 0644);
        if (fd < 0) {
            throw std::system_error(errno, std::system_category(),
                                    "Failed to open " + name);
        }
        hash_table_meta meta;
        if (read(fd, &meta, sizeof(meta)) != sizeof(meta)) {
            close(fd);
            throw std::system_error(errno, std::system_category(),
                                    "Failed to read " + name);
        }
        num_elements_ = meta.num_elements;

        // initialization
        size_t num_buckets = meta.num_buckets;
        num_threshold_ = size_t(num_buckets * (conf_.first_level_occupyncy_pct / 100.0));

        // alloc table
        table_holder_ = map_table(get_shm_basename() + ".tbl", num_buckets);
        table_.init(num_buckets, static_cast<table_type::table_type>(table_holder_.get()));

        total_elements_limit_ = kMaxMachineMemory / sizeof(value_type) / 2;

        // prepare for blocks
        size_block_ = min_buckets(0, total_elements_limit_ / kMaxBlocks + 1);
        if (size_block_ != meta.size_block) {
            throw std::runtime_error("Wrong size_block: " +
                                     std::to_string(meta.size_block) +
                                     " vs " + std::to_string(size_block_));
        }

        // alloc blocks
        assert(block_fd_ < 0);
        auto block_name = get_shm_basename() + ".blk";
        block_fd_ = shm_open(block_name.c_str(), O_RDWR, 0644);
        if (block_fd_ < 0) {
            close(fd);
            throw std::system_error(errno, std::system_category(),
                                    "Failed to shm_open " + block_name);
        }
        if (flock(block_fd_, LOCK_EX | LOCK_NB)) {
            close(fd);
            close(block_fd_);
            block_fd_ = -1;
            throw std::system_error(errno, std::system_category(),
                                    "Failed to lock " + block_name);
        }
        crowd_hash_blocks_ = new block_t *[kMaxBlocks];
        memset(crowd_hash_blocks_, 0, kMaxBlocks * sizeof(block_t *));
        begin_block_ = meta.begin_block;
        for (size_t i = 0; i < meta.num_blocks; ++i) {
            block_meta bmeta;
            if (read(fd, &bmeta, sizeof(bmeta)) != sizeof(bmeta)) {
                close(fd);
                close(block_fd_);
                block_fd_ = -1;
                throw std::system_error(errno, std::system_category(),
                                        "Failed to read " + name);
            }
            size_t size = roundup(sizeof(value_type) * size_block_,
                                  sysconf(_SC_PAGESIZE));
            void *ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                             MAP_SHARED | MAP_POPULATE, block_fd_,
                             block_offset_);
            if (ptr == MAP_FAILED) {
                close(fd);
                close(block_fd_);
                block_fd_ = -1;
                throw std::system_error(errno, std::system_category(),
                                        "Failed to map " + block_name);
            }
            block_offset_ += size;
            block_holders_.emplace_back(
                    ptr, [size](void *ptr) { munmap(ptr, size); });
            mem_size_ += sizeof(block_t) + size;
            crowd_hash_blocks_[block_holders_.size() - 1] =
                    new block_t(static_cast<value_type *>(block_holders_.back().get()), bmeta.num_elements,
                                size_block_, (1 << kBitProbes), 0, conf_.second_level_occupyncy_pct);
        }
        mem_size_ +=
                sizeof(table_type) + sizeof(spinned_item) * table_.size() + sizeof(block_t *) * kMaxBlocks +
                conf.num_buffer * conf.buffer_size;
        close(fd);
    }

    static std::string get_shm_basename(const double_hash_conf &conf) {
        auto pos = conf.file_name.rfind('/');
        if (pos != std::string::npos) {
            return conf.file_name.substr(pos);
        } else {
            return '/' + conf.file_name;
        }
    }

    std::string get_shm_basename() {
        return get_shm_basename(conf_);
    }

    ~double_hash() {
        if (block_fd_ >= 0) {
            try {
                save();
            }
            catch (const std::runtime_error &e) {
                shm_unlink((get_shm_basename() + ".meta").c_str());
                shm_unlink((get_shm_basename() + ".tbl").c_str());
                shm_unlink((get_shm_basename() + ".blk").c_str());
            }
            close(block_fd_);
        }
        if (crowd_hash_blocks_) {
            for (size_t i = 0; i != kMaxBlocks; ++i) {
                delete crowd_hash_blocks_[i];
                crowd_hash_blocks_[i] = NULL;
            }
            delete[] crowd_hash_blocks_;
            crowd_hash_blocks_ = NULL;
        }
    }

public:
    // Size
    size_t size() const { return num_elements_.load(std::memory_order_relaxed); }

    size_t mem_size() const { return mem_size_; }

    bool empty() const { return size() == 0; }

    size_t bucket_count() const { return table_.size(); }

    bool will_rehash() const { return size() > num_threshold_; }

    size_t total_elements_limit() const {
        return total_elements_limit_;
    }

    bool exceed_limit() const {
        return size() + 1 >= total_elements_limit();
    }

    /*
     * If we have more elements than num_threshold(), rehash will happen
     */
    size_t num_threshold() const {
        return num_threshold_;
    }

    // number of keys missing due to conflict while resizing
    size_t key_missing_num() const {
        return key_missing_num_;
    }

    // end accessor
    accessor end() const {
        assert(crowd_hash_blocks_ != NULL && "Call init first");
        return accessor(NULL);
    }

    /**
     * open the AOF file using a memory buffer
     *
     * @param buffer_size size of this memory buffer
     * @param offset where to start read this AOF file
     * @param num_elements number of to be read
     *
     * @return a file read iterator of this AOF file
     **/
    file_read_iterator buffered_open(
            size_t buffer_size, size_t offset = 0,
            size_t num_elements = std::numeric_limits<size_t>::max()) {
        assert(crowd_hash_blocks_ != NULL && "Call init first");
        return file_read_iterator(this, buffer_size, offset, num_elements);
    }

    // flush the data in the async_file buffer
    void flush() { file_it_->flush(); }

    // Find
    // return the accessor, if found
    // return end(), if not found
    accessor find(const key_type &key, uint64_t *lock_index = nullptr) const {
        assert(crowd_hash_blocks_ != NULL && "Call init first");
        iterator it;
        uint64_t hash_value = hash(key);
        if (lock_index)
            return find_in_table_with_lock(const_cast<double_hash *>(this)->table_, hash_value,
                                           sign(hash_value), &it, lock_index);
        else
            return find_in_table(const_cast<double_hash *>(this)->table_, hash_value, sign(hash_value), &it);
    }

    void erase(const key_type &) {
        assert(false && "erase is not supported");
    }

    // never shrink
    void min_load_factor(int) {}

    // Insert
    // return the accessor, if success or exist key.
    // return end(), if failed.
    //  exist will be set true, if there was the key already.
    accessor insert(const key_type &key, const data_type &data, bool *exist, uint64_t *lock_index = nullptr,
                    int thread_index = -1) {
        return insert(key, data, extra_type(), exist, lock_index, thread_index);
    }

    // TODO: difficult to add lock here, perhaps need to remove this interface?
    data_type &operator[](const key_type &key) {
        bool exist;
        auto result = insert(key, data_type(), &exist, nullptr);
        return result.get();
    }

    accessor insert(
            const key_type &key, const data_type &data, const extra_type &extra, bool *exist,
            uint64_t *lock_index = nullptr, int thread_index = -1) {
        if (exceed_limit()) {
            InvalidOperation io;
            io.err_code = parameter_server::ErrorCode::DOUBLE_HASH_EXCEED_LIMIT;
            io.why = "Should increase total_elements_limit";
            LOG(WARNING) << io.why;
            throw io;
        }
        assert(crowd_hash_blocks_ != NULL && "Call init first");
        uint64_t hash_value = hash(key);
        sign_type sign_value = sign(hash_value);
        iterator it;

        // resize if need
        if (size() > num_threshold_) {
            InvalidOperation io;
            io.err_code = parameter_server::ErrorCode::DOUBLE_HASH_NEED_REHASH;
            io.why = "double hash is full, need explicit resize, name: " + conf_.file_name;
            LOG(WARNING) << io.why;
            throw io;
        }

        // check exist value.
        accessor ac;
        if (lock_index) {
            ac = find_in_table_with_lock(table_, hash_value, sign_value, &it, lock_index);
        } else {
            ac = find_in_table(table_, hash_value, sign_value, &it);
        }
        if (ac != end()) // exist value
        {
            *exist = true;
            return ac;
        }
        // now we found the empty place
        // start insert the real data into block.
        bool succeed = false;
        {
            spinlock_guard guard(layer2_lock_);
            succeed = insert_into_block(hash_value, sign_value, data, &ac, &(*it));
        }
        if (!succeed) {
            if (it->lock.lock_)
                it->lock.unlock();
            if (lock_index != nullptr) {
                *lock_index = kInvalidLockIndex;
            }
            InvalidOperation io;
            io.err_code = parameter_server::ErrorCode::DOUBLE_HASH_EXCEED_LIMIT;
            io.why = "Insert failed, maybe you should increase total_elements_limit";
            LOG(WARNING) << io.why;
            throw io;
        }
        // success, increase the element number
        num_elements_.fetch_add(1, std::memory_order_relaxed);

        // dump key and extra to file
        file_it_->write(key, extra, thread_index);
        *exist = false;
        return ac;
    }

    size_t required_buckets(size_t elements_size, double allowance_rate = 0.0) {
        double occupyncy_rate = conf_.first_level_occupyncy_pct / 100.0 * (1 - allowance_rate);
        size_t num_buckets = min_buckets(elements_size, bucket_count());
        while (size_t(num_buckets * occupyncy_rate) <= elements_size) {
            assert(static_cast<size_t>(num_buckets * 2) > num_buckets);
            num_buckets *= 2;
        }
        return num_buckets;
    }

    bool need_rehash(size_t elements_size, double allowance_rate = 0.0) {
        return (required_buckets(elements_size, allowance_rate) > bucket_count());
    }

    void rehash(size_t elements_size, double allowance_rate = 0.0) {
        size_t num_buckets = required_buckets(elements_size, allowance_rate);
        if (num_buckets > bucket_count()) {
            resize(num_buckets);
        }
    }

    // resize to at least new_buckets.
    void resize(size_t new_buckets) {
        assert(crowd_hash_blocks_ != NULL && "Call init first");
        size_t bucket_count = min_buckets(0, new_buckets);
        table_type tmp_table;
        iterator it, new_it;

        file_it_->flush();
        auto read_it = buffered_open(1000000);
        memory_holder_type tmp_table_holder;

        // 内存余量检查
        if (!memory_available_is_enough_checker_(bucket_count * sizeof(item_type))) {
            neo::InvalidOperation io;
            io.err_code = parameter_server::ErrorCode::NOT_ENOUGH_MEMORY;
            io.why = "resize cannot not allocate tmp_table "
                     "since available memory is not enough";
            LOG(WARNING) << io.why;
            throw io;
        }

        // alloc new table
        if (table_holder_) {
            shm_unlink((get_shm_basename() + ".tbl").c_str());
            tmp_table_holder = map_table(get_shm_basename() + ".tbl", bucket_count, true);
            tmp_table.init(bucket_count, static_cast<table_type::table_type>(tmp_table_holder.get()), true);
        } else {
            tmp_table.init(bucket_count);
        }
        assert(tmp_table.size() != table_.size() && "resize error");
        num_threshold_ = size_t(bucket_count * (conf_.first_level_occupyncy_pct / 100.0));

        // copy to the new table
        while (read_it.read_key()) {
            uint64_t hash_value = hash(read_it->key);
            sign_type sign_value = sign(hash_value);
            if (find_in_table(table_, hash_value, sign_value, &it) == end()) {
                ++key_missing_num_;
                continue;
            }
            if (find_in_table(tmp_table, hash_value, sign_value, &new_it) != end()) {
                ++key_missing_num_;
                continue;
            }
            *new_it = *it;
        }
        mem_size_ += sizeof(spinned_item) * (tmp_table.size() - table_.size());
        table_.swap(tmp_table);
        std::swap(table_holder_, tmp_table_holder);
        // table_ will be freed and file will be closed
    }

    sign_type key_info(const accessor ac) const {
        if (ac == end())
            return 0;
        return key_info(ac.get_all().first);
    }

    void set_key_info(accessor iter) {
        if (iter != end())
            set_key_info(&iter.get_all().first);
    }

    void clear_key_info(accessor iter) {
        if (iter != end())
            clear_key_info(&iter.get_all().first);
    }

    void unlock(uint64_t lock_index) {
        if (likely(lock_index < table_.size())) {
            auto it = table_.get(lock_index);
            it->lock.unlock();
        }
    }

private:
    sign_type key_info(const sign_type &sign) const {
        return sign >> kSignBit;
    }

    void set_key_info(sign_type *sign) {
        *sign |= (1u << kSignBit);
    }

    void clear_key_info(sign_type *sign) {
        *sign &= kSignMask;
    }

    size_t min_buckets(size_t num_elts, size_t min_buckets_wanted) {
        size_t sz = 32;
        assert(conf_.first_level_occupyncy_pct > 0.05);
        while (sz < min_buckets_wanted ||
               num_elts >= size_t(sz * (conf_.first_level_occupyncy_pct - 0.05) / 100.0)) {
            assert(static_cast<size_t>(sz * 2) > sz);
            sz *= 2;
        }
        return sz;
    }

    // insert in to the block
    // return false, if failed
    // reutrn true, if success, pac is point to the element just inserted
    bool insert_into_block(uint64_t hash_value, sign_type sign_value, data_type data, accessor *pac,
                           spinned_item *item) {
        // first trying, maybe modify begin_block_
        block_t *block;
        if ((block = alloc_block(&begin_block_)) == NULL) {
            return false;   // failed
        }
        item_type num_block = begin_block_;
        size_t probe;
        const sign_type sign_value_masked = sign_value & kSignMask;
        while (block->insert(hash_value, sign_value_masked, data, &probe, pac) == false) {
            assert(probe != 0); // cannot be an exist value.
            // must probe = max_probes, try to next block.
            num_block++;
            if ((block = alloc_block(&num_block)) == NULL) {
                return false;  // failed
            }
        }
        item->make_item(num_block, probe, sign_value & spinned_item::kMaskSign);
        return true;
    }

    // find or alloc a block that is not null and not full.
    // num_block will be changed.
    // return pointer to the block, null if fails
    block_t *alloc_block(item_type *num_block) {
        if (*num_block >= kMaxBlocks) {
            return NULL;
        }
        block_t *block;
        while (true) {
            block = crowd_hash_blocks_[*num_block];
            if (block == NULL) {
                // 内存余量检查
                if (!memory_available_is_enough_checker_(sizeof(block_t) + sizeof(value_type) * size_block_)) {
                    neo::InvalidOperation io;
                    io.err_code = parameter_server::ErrorCode::NOT_ENOUGH_MEMORY;
                    io.why = "alloc_block found not enough available memory "
                             "before initializing new block_t";
                    LOG(WARNING) << io.why;
                    throw io;
                }

                if (block_fd_ >= 0) {
                    size_t size = roundup(sizeof(value_type) * size_block_,
                                          sysconf(_SC_PAGESIZE));
                    if (ftruncate(block_fd_, block_offset_ + size)) {
                        throw std::bad_alloc();
                    }
                    void *ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                                     MAP_SHARED | MAP_POPULATE, block_fd_,
                                     block_offset_);
                    if (ptr == MAP_FAILED) {
                        throw std::bad_alloc();
                    }
                    block_offset_ += size;
                    block_holders_.emplace_back(
                            ptr, [size](void *ptr) { munmap(ptr, size); });
                    block = new block_t(static_cast<value_type *>(block_holders_.back().get()), 0,
                                        size_block_, (1 << kBitProbes), 0,
                                        conf_.second_level_occupyncy_pct, true);
                    crowd_hash_blocks_[*num_block] = block;
                    mem_size_ += sizeof(block_t) + size;
                } else {
                    block = new block_t(size_block_, (1 << kBitProbes), 0,
                                        conf_.second_level_occupyncy_pct);
                    crowd_hash_blocks_[*num_block] = block;
                    mem_size_ += sizeof(block_t) + sizeof(value_type) * size_block_;
                }
                break;
            } else if (block->full()) {
                (*num_block)++;
                if (*num_block >= kMaxBlocks) {
                    block = NULL;
                    break;
                }
                continue;
            }
            break;
        }
        return block;
    }

    // get value from crowd_hash_blocks_
    accessor get_value(const uint64_t hash_value, const iterator &it) const {
        return crowd_hash_blocks_[it->block()]->find(hash_value, it->probe());
    }

    // accessor return end(), if there is a empty place.
    //          otherwise ac is returned point to the exist value.
    //          both situlation, iterator will be set.
    accessor find_in_table(table_type &tb, const uint64_t hash_value, const sign_type sign_value,
                           iterator *pit) const {
        size_t num_probes = 0;
        const size_t bucket_count_minus_one = tb.size() - 1;
        size_t bucknum = hash_value & bucket_count_minus_one;
        accessor ac;
        iterator it;

        const sign_type sign_value_masked = sign_value & kSignMask;
        const sign_type layer1_sign_value = sign_value & spinned_item::kMaskSign;
        while (true) {
            it = tb.get(bucknum);
            if (it->empty())        // empty?
            {
                ac = end();
                break;
            }
            if (it->sign() == layer1_sign_value) {
                ac = get_value(hash_value, it);
                if ((ac.get_all().first & kSignMask) == sign_value_masked)   // exist value.
                {
                    break;
                }
            }
            ++num_probes;
            bucknum = (bucknum + num_probes) & bucket_count_minus_one;
            assert(num_probes < bucket_count() && "hashtable is full");
        }
        *pit = it;
        return ac;
    }

    // accessor return end(), if there is a empty place.
    //          otherwise ac is returned point to the exist value.
    //          both situlation, iterator will be set.
    accessor find_in_table_with_lock(table_type &tb, const uint64_t hash_value, const sign_type sign_value,
                                     iterator *pit, uint64_t *lock_index) const {
        size_t num_probes = 0;
        const size_t bucket_count_minus_one = tb.size() - 1;
        size_t bucknum = hash_value & bucket_count_minus_one;
        accessor ac;
        iterator it;

        const sign_type sign_value_masked = sign_value & kSignMask;
        const sign_type layer1_sign_value = sign_value & spinned_item::kMaskSign;
        while (true) {
            it = tb.get(bucknum);
            if (it->empty())        // empty?
            {
                it->lock.lock();
                if (likely(it->empty()))  // double check in case concurrent insert happens
                {
                    ac = end();
                    break;
                } else {
                    it->lock.unlock();
                }
            }
            if (it->sign() == layer1_sign_value) {
                ac = get_value(hash_value, it);
                if ((ac.get_all().first & kSignMask) == sign_value_masked)   // exist value.
                {
                    it->lock.lock();
                    break;
                }
            }
            ++num_probes;
            bucknum = (bucknum + num_probes) & bucket_count_minus_one;
            assert(num_probes < bucket_count() && "hashtable is full");
        }
        *lock_index = bucknum;
        *pit = it;
        return ac;
    }

    uint64_t hash(const key_type &key) const {
        return hash_func_.operator()(key);
    }

    sign_type sign(uint64_t key) const {
        return sign_func_.operator()(key);
    }

public:
    // configure
    double_hash_conf conf_;

private:
    static constexpr sign_type kSignBit = sizeof(sign_type) * 8 - 1;
    static constexpr sign_type kSignMask = (1u << kSignBit) - 1;
    static constexpr size_t kMaxBlocks = spinned_item::kMaxBlocks;
    static constexpr size_t kBitProbes = spinned_item::kBitProbes;
    static constexpr size_t kMaxMachineMemory = size_t(500) * 1024 * 1024 * 1024; // 500GB
    // Actual data
    std::atomic<size_t> num_elements_;
    size_t mem_size_;
    size_t num_threshold_;

    struct hash_table_meta {
        size_t size_block;
        size_t num_buckets;
        size_t num_elements;
        size_t num_blocks;
        size_t begin_block;
    };

    struct block_meta {
        size_t num_elements;
    };

    // table
    memory_holder_type table_holder_;
    table_type table_;

    // blocks
    int block_fd_ = -1;
    off_t block_offset_ = 0;
    std::vector<memory_holder_type> block_holders_;
    block_t **crowd_hash_blocks_ = nullptr;
    item_type begin_block_;
    size_t size_block_ = 0;             // sub_hash buckets count

    // hash and sign functional object
    HashFunc hash_func_;
    SignFunc sign_func_;

    // file iterator
    std::unique_ptr<double_hash_file_write_iterator> file_it_;
    size_t total_elements_limit_;
    size_t key_missing_num_;

    // 输入需要申请的内存量 in bytes, return true iff 有足够的余量
    std::function<bool(size_t)> memory_available_is_enough_checker_;

    // layer2 alloc lock
    spinlock_t layer2_lock_;
};

} // namespace neo
}
#endif // ZEPHYR_DOUBLEHASH_H_