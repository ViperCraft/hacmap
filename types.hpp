#pragma once

#include "memory.hpp"
#include <stdint.h>
#include <iostream>
#include <type_traits>

#define DEFAULT_PAGE_SIZE 4096
#define DBG(x) {}


namespace utils {

inline uint32_t maxbits(unsigned int const v)
{
    return v == 0 ? 0 : 32 - __builtin_clz(v);
}

inline uint32_t maxbits(unsigned long int const v)
{
    return v == 0 ? 0 : 64 - __builtin_clzl(v);
}

inline uint32_t maxbits(unsigned long long int const v)
{
    return v == 0 ? 0 : 64 - __builtin_clzll(v);
}
   
}

int const MAX_OFFSET_BITS = 43; 
int const MAX_KEYS_IN_BUCKET = 21; 

struct BucketEntry
{
    uint64_t    offset : 43; // up to 8 ТB storage size, about 2 trillons of keys
    uint32_t    nkeys  : 21; // up to 2 million keys in bucket, usually not more than 1K
};

struct BucketEntryTiny
{
    uint32_t    offset : 23; // up 64GB of storage size, if each buket is aligned to 8 bytes
    uint32_t    nkets  : 9; // max 512 records per bucket only!
};

static_assert( sizeof(BucketEntry) == 8, "BucketEntry must fit into 8 bytes!" );
static_assert( sizeof(BucketEntryTiny) == 4, "BucketEntry must fit into 4 bytes!" );

namespace detail {

template<typename Key, typename Value>
struct KVCheck
{
    static_assert( std::is_integral<Key>::value, "Key type need to be integral!" );
    static_assert( sizeof(Key) == 4 || sizeof(Key) == 8, "Key packing only support for 32-bit or 64-bit values" );
};

inline uint32_t calc_buckets_count( size_t kv_sz_total, size_t const page_size )
{
    if( kv_sz_total )
    {        
        size_t ratio = kv_sz_total > page_size ? kv_sz_total / page_size : 1;
        
        return 1U << (64 - __builtin_clzl(ratio) ); 
    }
    
    return 0;
}

class BucketIndex
{
private:
    static size_t read_nbuckets( utils::MemoryReader &rdr, uint32_t &key_bits_store )
    {
        // read from footer nbuckets of the stream end
        rdr.seek(rdr.size() - 1);

        uint8_t nbucket_p2;
        
        rdr >> nbucket_p2;


        uint8_t _key_bits_store;

        // detect if higher bit is set then
        // we also need to get previuos byte too
        if( nbucket_p2 & 0x80 )
        {
            // clear bit
            nbucket_p2 &= ~0x80;
            rdr.seek_by(-2L);
            rdr >> _key_bits_store;
            key_bits_store = _key_bits_store;
        }
        else
            key_bits_store = 0; // not defined!

        size_t nbuckets = 1UL << nbucket_p2;

        return nbuckets;
    }
public:
    /*
        init from istream
     */
    BucketIndex( utils::MemoryReader rdr )
        : nbuckets_(read_nbuckets(rdr, key_bits_store_))
        , data_(rdr.get_ownership())
    {
        dstart_ = data_.get_ptr<uint8_t const>();
    }
    
    size_t get_mask() const { return nbuckets_ - 1; }
    size_t get_nbuckets() const { return nbuckets_; }
    size_t get_key_bits_store() const { return key_bits_store_; }
    
    // return number of records!
    size_t size() const
    {
        if( nbuckets_ )
        {
            size_t nrec = 0;
            auto const *be = get_entries();
            for( size_t i = 0; i < nbuckets_; ++i )
               nrec += be[i].nkeys;
            return nrec;
        }
        return 0;
    }

    uint8_t const *get_data_start() const
    {
        return dstart_;
    }

    BucketEntry const *get_entries() const
    {
        return reinterpret_cast<BucketEntry const *>(get_data_start());
    }
    
    // return bucket entry[offset, nkeys] by bucket index
    BucketEntry get(size_t i) const
    {
        assert( i < nbuckets_ );
        return get_entries()[i];
    }

    std::pair<uint8_t const*, uint32_t> get_unpacked(size_t i) const
    {
        assert( i < nbuckets_ );
        BucketEntry be = get_entries()[i];
        return std::make_pair(dstart_ + be.offset, uint32_t(be.nkeys));
    }
    
    size_t get_mem_size() const
    {
        return data_.get_mem_size();
    }

    size_t get_compressed_keys_size( uint32_t nrec ) const
    {
        return get_kcompressed_size(nrec, key_bits_store_);
    }

public:
    static size_t get_kcompressed_size( uint32_t nrecords, uint32_t key_bits_store )
    {
        size_t total_bits = nrecords * key_bits_store;
        return ((total_bits - 1) / 64 + 1) * 8;
    }
private:
    uint8_t const           *dstart_;
    size_t const            nbuckets_;
    uint32_t                key_bits_store_;
    utils::MemoryHolder     data_;
};

} // namespace detail



