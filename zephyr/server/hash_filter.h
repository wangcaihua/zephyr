/**
 * @file hash_filter.h
 * @author qiaomu(qiaomu@bytedance.com)
 * @date 08/14/2014 09:54:53 AM
 * @brief
 *
 **/

#ifndef HASH_FILTER_H_
#define HASH_FILTER_H_

#include <fstream>
#include <limits>
#include <glog/logging.h>
#include "matrix/common/hash_func.h"
#include "matrix/common/types.h"
#include "idl/matrix/proto/model.pb.h"
#include "matrix/file_accessor.h"
#include "idl/parameter_server/parameter_server_types.h"
#include "filter.h"
#include "parameter_server/common/utils.hpp"

namespace zephyr {
namespace server {

template <typename DATA>
class HashFilter;
template <typename DATA>
class HashFilterIterator {
    friend class HashFilter<DATA>;

public:
    HashFilterIterator(): filter_(NULL), pvalue_(NULL), sign_(0)
    {}
    uint32_t add(uint32_t add_count)
    {
        assert(valid() && "check validation before add");
        if(add_count > HashFilter<DATA>::max_count())
            add_count = HashFilter<DATA>::max_count();
        if(*pvalue_ == 0)
        {
            ++filter_->num_elements_;
            if(filter_->num_elements_ > filter_->capacity_)
            {
                filter_->num_elements_ = filter_->capacity_;
                /*
                InvalidOperation io;
                io.why = "total elements exceeds filter capacity";
                throw io;
                */
            }
            *pvalue_ = (sign_ << HashFilter<DATA>::count_bit) + add_count;
            return 0;
        }
        unsigned char count = *pvalue_ & HashFilter<DATA>::max_count();
        if(count + add_count >= HashFilter<DATA>::max_count())
            *pvalue_ |= HashFilter<DATA>::max_count();
        else
            *pvalue_ += add_count;
        return count;
    }

    uint32_t get() const
    {
        if(!pvalue_)
            return HashFilter<DATA>::max_count();
        return *pvalue_ & HashFilter<DATA>::max_count();
    }

    bool valid() const
    {
        return pvalue_ != NULL;
    }

    bool empty() const
    {
        assert(valid() && "check validation before empty");
        return *pvalue_ == 0;
    }
private:
    explicit HashFilterIterator(HashFilter<DATA>* filter, DATA* pvalue, DATA sign):
            filter_(filter), pvalue_(pvalue), sign_(sign)
    {}
    HashFilter<DATA>* filter_;
    DATA* pvalue_;
    DATA sign_;
};

template <typename DATA>
class HashFilter: public Filter
{
    friend class HashFilterIterator<DATA>;
public:
    explicit HashFilter(size_t capacity, double fill_rate=1.5):
            failure_count_(0), total_size_(capacity * fill_rate),
            num_elements_(0), fill_rate_(fill_rate)
    {
        capacity_ = capacity;
        map_.resize(total_size_ + MAX_STEP, 0);
    }

    uint32_t add(FID fid, uint32_t count)
    {
        HashFilterIterator<DATA> iter = find(fid, MAX_STEP);
        if(iter.valid())
        {
            return iter.add(count);
        }
        failure_count_ += 1;
        return max_count();
    }

    uint32_t get(FID fid) const
    {
        return const_cast<HashFilter*>(this)->find(fid, MAX_STEP).get();
    }

    HashFilterIterator<DATA> find(FID fid, int max_step)
    {
        assert(max_step <= MAX_STEP && "illegal max_step");
        DATA sign = signature(fid);
        int step = 0;
        size_t hash_value = hash(fid) % total_size_;
        DATA* pvalue = reinterpret_cast<DATA*>(&map_[hash_value]);
        do
        {
            if(*pvalue == 0 || (*pvalue >> count_bit) == sign)
            {
                return HashFilterIterator<DATA>(this, pvalue, sign);
            }
            ++pvalue;
        } while(++step < max_step);
        return HashFilterIterator<DATA>(this, NULL, sign);
    }

    bool full() const
    {
        return num_elements_ >= capacity_ - 1;
    }

    bool dump(
            const std::string& dump_path, const std::string &model_name,
            int64_t rate_limit, const uint64_t operation_token) const
    {
        auto dump_is_allowed = [this, &dump_path](const uint64_t operation_token) -> bool {
            if (!parameter_server::Coordinator::get_instance()->operation_is_allowed(
                    operation_token))
            {
                LOG(ERROR) << "hash_filter dump on path [" <<
                           dump_path << "] failed since being stopped";
                return false;
            }
            return true;
        };
        if (!dump_is_allowed(operation_token))
        {
            return false;
        }

        // 清理环境
        matrix::FileAccessor::remove(dump_path, &operation_token);

        // 备份filter meta
        if (!dump_is_allowed(operation_token))
        {
            return false;
        }
        proto::HashFilterMeta meta;
        meta.set_total_size(total_size_);
        meta.set_num_elements(num_elements_);
        meta.set_capacity(capacity_);
        std::string buffer;
        matrix::FileAccessor fa;
        std::ostream *poutput;
        uint32_t total_pack_count{0};
        try
        {
            poutput = fa.open_write(dump_path, true, rate_limit, &operation_token);
        }
        catch(InvalidOperation io)
        {
            LOG(WARNING) << io.why;
            return false;
        }

        uint32_t checksum{0};
        if (!parameter_server::write_proto(meta, &buffer, poutput, model_name, &checksum))
        {
            LOG(ERROR) << "hashfilter dump meta to path[" << dump_path << "] failed";
            return false;
        }

        // checksum for filter meta
        if (!dump_is_allowed(operation_token))
        {
            return false;
        }
        proto::HashFilterValue values;
        values.set_is_checksum(true);
        values.set_checksum(checksum);
        if (!parameter_server::write_proto(values, &buffer, poutput, model_name, nullptr))
        {
            LOG(ERROR) << "hashfilter dump meta checksum to path[" << dump_path << "] failed";
            return false;
        }
        values.Clear();

        // 备份filter 内容
        if (!dump_is_allowed(operation_token))
        {
            return false;
        }
        for(DATA value : map_)
        {
            values.add_value(value);
            if(values.value_size() > DUMP_VALUE_SIZE)
            {
                // dump data
                if (!dump_is_allowed(operation_token))
                {
                    return false;
                }
                if (!parameter_server::write_proto(values, &buffer, poutput, model_name, &checksum))
                {
                    LOG(ERROR) << "hashfilter dump content into path[" << dump_path << "] failed";
                    return false;
                }
                ++total_pack_count;
                values.Clear();

                // dump checksum
                if (!dump_is_allowed(operation_token))
                {
                    return false;
                }
                values.set_is_checksum(true);
                values.set_checksum(checksum);
                if (!parameter_server::write_proto(values, &buffer, poutput, model_name, nullptr))
                {
                    LOG(ERROR) << "hashfilter dump content checksum into path[" << dump_path << "] failed";
                    return false;
                }
                ++total_pack_count;
                values.Clear();
            }
        }

        // dump remaining filter data
        if(values.value_size() > 0)
        {
            // dump data
            if (!dump_is_allowed(operation_token))
            {
                return false;
            }
            if (!parameter_server::write_proto(values, &buffer, poutput, model_name, &checksum))
            {
                LOG(ERROR) << "hashfilter dump remaining content into path[" << dump_path << "] failed";
                return false;
            }
            ++total_pack_count;
            values.Clear();

            // dump checksum
            if (!dump_is_allowed(operation_token))
            {
                return false;
            }
            values.set_is_checksum(true);
            values.set_checksum(checksum);
            if (!parameter_server::write_proto(values, &buffer, poutput, model_name, nullptr))
            {
                LOG(ERROR) << "hashfilter dump remaining content checksum into path[" << dump_path << "] failed";
                return false;
            }
            ++total_pack_count;
            values.Clear();
        }

        if (poutput) {
            if (!dump_is_allowed(operation_token))
            {
                return false;
            }

            // dump eof
            values.set_is_eof(true);
            values.set_total_pack_count(total_pack_count);
            if (!parameter_server::write_proto(values, &buffer, poutput, model_name, nullptr))
            {
                LOG(ERROR) << "hashfilter dump eof into path[" << dump_path << "] failed";
                return false;
            }
            values.Clear();

            // flush
            poutput->flush();
            if (!*(poutput)) {
                LOG(ERROR) << "hashfilter dump flush content into path[" << dump_path << "] failed";
                return false;
            }
        }
        else
        {
            LOG(ERROR) << "poutput is not valid before flush hash_filter; " <<
                       "THIS SHOULD NOT HAPPEN";
            return false;
        }
        return true;
    }

    bool load(
            const std::string& path, const std::string& model_name,
            int64_t rate_limit, const uint64_t operation_token,
            const int dump_version)
    {
        LOG(INFO) << "load filter from path[" << path << "]";
        if(!parameter_server::Coordinator::get_instance()->operation_is_allowed(operation_token))
        {
            return false;
        }

        matrix::FileAccessor fa;
        std::istream *pinput = fa.open_read(path, true, rate_limit, &operation_token);

        // loading filter meta
        int buffer_size = DUMP_VALUE_SIZE * sizeof(DATA) * 4;
        std::unique_ptr<char[]> buffer(new char[buffer_size]);
        proto::HashFilterMeta meta;
        uint32_t checksum{0};
        if(!parameter_server::read_proto(&meta, buffer.get(), buffer_size, pinput, model_name, &checksum))
        {
            LOG(ERROR) << "load: invalid hash filter: " + path;
            return false;
        }
        total_size_ = meta.total_size();
        num_elements_ = meta.num_elements();
        capacity_ = meta.capacity();
        parameter_server::Coordinator::get_instance()->check_memory(
                (total_size_ + MAX_STEP) * sizeof(DATA), true, false, true);
        map_.resize(total_size_ + MAX_STEP, 0);

        // filter meta 校验
        proto::HashFilterValue values;
        if (0 != dump_version)
        {
            if (!parameter_server::Coordinator::get_instance()->operation_is_allowed(operation_token))
            {
                return false;
            }

            if(!parameter_server::read_proto(&values, buffer.get(), buffer_size, pinput, model_name, nullptr))
            {
                LOG(ERROR) << "load filter meta checksum: invalid hash filter: " + path;
                return false;
            }
            if (!values.is_checksum() || values.checksum() != checksum)
            {
                LOG(ERROR) << "filter loading; checksum for model[" << model_name <<
                           "] failed; is_checksum[" << values.is_checksum() <<
                           "] must be true; expected_checksum[" << values.checksum() <<
                           "] actual_checksum[" << checksum << "]";
                return false;
            }
            values.Clear();
        }

        // loading filter content
        size_t curr_index = 0;
        uint32_t checksum_for_data_pack{0};
        uint32_t checksum_for_current_pack{0};
        uint32_t pack_cnt{0};
        while(pinput && parameter_server::read_proto(&values, buffer.get(), buffer_size,
                                                     pinput, model_name, &checksum_for_current_pack))
        {
            ++pack_cnt;
            if (!parameter_server::Coordinator::get_instance()->operation_is_allowed(operation_token))
            {
                return false;
            }

            // 校验
            if (0 != dump_version)
            {
                if ((pack_cnt & 0x1) == 0)
                {
                    // 校验上一个数据包
                    if (!values.is_checksum() || values.checksum() != checksum_for_data_pack)
                    {
                        LOG(ERROR) << "load filter content checksum failed; " <<
                                   "is_checksum[" << values.is_checksum() << "] should be true; " <<
                                   "actual_checksum[" << values.checksum() << "] expected_checksum[" <<
                                   checksum_for_data_pack << "] model[" << model_name << "]";
                        return false;
                    }
                    values.Clear();
                    continue;
                }
                else if (values.is_eof())
                {
                    // 遇到eof包, 校验读入的包数
                    if (values.total_pack_count() != pack_cnt - 1)
                    {
                        LOG(ERROR) << "load filter content eof check failed; " <<
                                   "actual_pack_cnt[" << pack_cnt - 1 <<
                                   "] expected_pack_cnt[" << values.total_pack_count() << "]" <<
                                   " model[" << model_name << "]";
                        return false;
                    }
                    else
                    {
                        // 成功结束
                        return true;
                    }
                }
                else
                {
                    // 当前是数据包
                    checksum_for_data_pack = checksum_for_current_pack;
                }
            }

            int i = 0;
            for(i = 0; i != values.value_size() && curr_index != map_.size(); ++i)
            {
                map_[curr_index++] = values.value(i);
            }
            if(i != values.value_size() && curr_index == map_.size())
            {
                LOG(ERROR) << "load: corrupted hash filter: " + path;
                return false;
            }

            values.Clear();
        }
        if(curr_index != map_.size())
        {
            std::cout << curr_index << "=curr_index map_size=" << map_.size() << std::endl;
            LOG(ERROR) << "load: corrupted hash filter: " + path;
            return false;
        }

        if (0 != dump_version)
        {
            // 在携带校验的备份数据上，一定要看到eof，否则视为失败
            return false;
        }
        return pinput->eof();
    }

    // TODO make this async
    void async_clear()
    {
        fill(map_.begin(), map_.end(), 0);
        num_elements_ = 0;
        failure_count_ = 0;
    }

    uint32_t size_mb() const
    {
        return map_.size() * sizeof(DATA) / 1024.0 / 1024.0;
    }

    static size_t size_byte(size_t capacity, double fill_rate=1.5)
    {
        return capacity * sizeof(DATA) * fill_rate + MAX_STEP;
    }

    uint64_t failure_count() const
    {
        return failure_count_;
    }

    DATA signature(FID fid) const
    {
        return (fid >> 17 | fid << 15) & sign_mask;
    }

    size_t estimated_total_element() const
    {
        return num_elements_;
    }

    bool exceed_limit() const override
    {
        return num_elements_ >= capacity_;
    }

    HashFilter* clone() const
    {
        return new HashFilter(*this);
    }

    bool operator==(const HashFilter& other) const
    {
        return total_size_ == other.total_size_ && \
           num_elements_ == other.num_elements_ && \
           capacity_ == other.capacity_ && \
           fill_rate_ == other.fill_rate_ && \
           map_ == other.map_;
    }

    constexpr static DATA sign_mask = ((1 << (sizeof(DATA) * 8)) - 1) >> count_bit;
private:
    constexpr static int DUMP_VALUE_SIZE = 1024 * 1024 * 20; // 10-20MB
    constexpr static int MAX_STEP = 64;
    size_t hash(FID fid) const
    {
        return hash_(fid);
    }
    murmurhash64<FID> hash_;
    std::vector<DATA> map_;
    uint64_t failure_count_;
    uint64_t total_size_;
    uint64_t num_elements_;
    double fill_rate_;
};

}
}

#endif  // HASH_FILTER_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

