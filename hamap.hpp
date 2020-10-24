#pragma once

#include "types.hpp"
#include <iostream>
#include <algorithm>


namespace detail {

template<typename Key>
inline Key const* binary_locate( Key const k, Key const * __restrict__ keys, uint32_t u )
{
    uint32_t l = 0;
    
    while (l < u)
    {
        uint32_t i = (l + u) >> 1;
        
        if (keys[i] > k)
            u = i;
        else if (keys[i] < k)
            l = i + 1;
        else {
            return keys + i;
        }
    }
    
    return nullptr;
}

} // namespace detail

/****************************************/
/* ExternHashing MAP                    */
/* First level is ordinal:              */
/* HASH-TABLE n-bits                    */
/* pointing into 2**n buckets           */
/* each bucket then with ordered keys   */
/****************************************/ 

template<typename Key, typename Value>
class HAMapIndexer : private detail::KVCheck<Key, Value>
{
private:
    typedef std::pair<Key, Value>               kv_pair_t;
    typedef std::vector< kv_pair_t >            bucket_kv_array_t;
    typedef std::vector< bucket_kv_array_t >    bucket_array_t;
    typedef std::vector< kv_pair_t >            unsorted_records_list_t;
public:
    /* pass here total number of records will be indexed or zero if you don't know */
    HAMapIndexer
    ( 
        size_t total_records_known_at_creation = 0UL, 
        size_t const page_size = DEFAULT_PAGE_SIZE
    )
        : buckets_( detail::calc_buckets_count(sizeof(kv_pair_t) * total_records_known_at_creation
                    , page_size) )
        , hash_mask_( !buckets_.empty() ? buckets_.size() - 1 : 0 )
        {
            DBG( std::cerr << "HAMapIndexer nbuckets=" << buckets_.size() << std::endl );
        }
    
    void add( std::pair<Key, Value> const &p )
    {
        add(p.first, p.second);
    }

    size_t get_hash_mask() const { return hash_mask_; }

    bucket_kv_array_t const& get_bucket_arr( size_t i ) const { return buckets_[i]; }
    
    void add( Key k, Value v )
    {
        if( !buckets_.empty() )
        {
            size_t const bucket_idx = k & hash_mask_;
            buckets_[bucket_idx].emplace_back(k, v);
        }
        else
        {
            unsorted_records_.emplace_back(k, v);
        }
    }

    void clear()
    {
        unsorted_records_.clear();
        unsorted_records_.shrink_to_fit();
    }
    
    /* WARN: size calculated on the fly! */
    size_t size() const
    {
        size_t sz = 0;
        if( !buckets_.empty() )
        {
            for( auto const &b : buckets_ )
                sz += b.size();
        }
        else
            sz = unsorted_records_.size();
        
        return sz;
    }
    
    std::vector<uint8_t> get_compacted( size_t const page_size = DEFAULT_PAGE_SIZE )
    {
        std::vector<uint8_t> buffer;
        utils::OStreamProxy os(buffer);
        compact_and_store(os, page_size);
        return buffer;
    }

    void compact_and_store( utils::OStreamProxy &os, size_t const page_size )
    {
        if( !buckets_.empty() )
        {
            // use predefined set size
            flush_buckets(os, buckets_);
        }
        else
        {
            // make index from unsorted_records_list_t
            size_t const nrec = unsorted_records_.size();
            
            // always return pow of 2 nbuckets value!
            size_t const nbuckets = detail::calc_buckets_count((sizeof(Key) + sizeof(Value)) * nrec, page_size);
            size_t const hash_mask = nbuckets - 1;
            
            bucket_array_t buckets(nbuckets);
            
            for( auto const & p : unsorted_records_ )
            {
                size_t const bucket_idx = p.first & hash_mask;
                buckets[bucket_idx].emplace_back(p);
            }

            os.prealloc
            (
                sizeof(BucketEntry) * buckets.size() 
                + nrec * (sizeof(Key) + sizeof(Value))
                + 1 /* footer */
            );
            
            flush_buckets(os, buckets);
        }
    }
    
private:
    
    static void flush_bucket( utils::OStreamProxy &os, bucket_kv_array_t &b )
    {
        // each bucket must sorted by key before get flushed
        std::sort(b.begin(), b.end());
        
        // store keys and values separatly
        
        os.write_range(b.begin(), b.end(), []( kv_pair_t const &p ) { return p.first; });
        
        os.write_range(b.begin(), b.end(), []( kv_pair_t const &p ) { return p.second; });

        // and apply bit packing for each range
        
    }
    
    static void flush_buckets( utils::OStreamProxy &os, bucket_array_t &buckets )
    {
        size_t const nbuckets = buckets.size();
        uint8_t n = 0;
        if( nbuckets )
        {
            // write buckets index
            BucketEntry be;
            uint64_t offs = sizeof(BucketEntry) * buckets.size();
            for( auto const &b : buckets )
            {
                uint32_t nkeys = b.size();
                be.offset = offs;
                be.nkeys = nkeys;
                os << be;
                offs += nkeys * (sizeof(Key) + sizeof(Value));
            }

            // write each bucket
            for( auto &b : buckets )
                flush_bucket(os, b);

            // use footer, to keep alignment fine.
            // write footer
            // store only N from: buckets = 2 ** (N - 1)
            n = utils::maxbits(nbuckets) - 1;
        }
        os << n;
    }
    
private:
    unsorted_records_list_t             unsorted_records_;
    bucket_array_t                      buckets_;
    size_t const                        hash_mask_;
};

template<typename Key, typename Value>
class HAMapSearcher : private detail::KVCheck<Key, Value>
{
public:
    /* construct searcher from readable std::istream interface
     */
    HAMapSearcher( std::istream &is )
        : bi_(is)
        , mask_(bi_.get_mask())
        {}
    
    /* usefull for tests and other */
    HAMapSearcher( HAMapIndexer<Key, Value> &idx )
        : bi_(idx.get_compacted())
        , mask_(bi_.get_mask())
        {}
    
    // unique key mode(get first equal key)
    // return pointer to found value or nullptr if not found!
    Value const* search( Key k ) const
    {
        auto const o = bi_.get( k & mask_ );
        // convert it into key offsets
        Key const *start = reinterpret_cast<Key const*>(bi_.get_data_start() + o.offset);
        
        auto it = detail::binary_locate(k, start, o.nkeys);

        if( nullptr != it )
        {
            // retrieve value by offset
            // detect offset value by iter
            size_t const offs = std::distance(start, it);
            // this is uncompressed version so just increment keys pointer
            Value const *value_ptr = reinterpret_cast<Value const*>(start + o.nkeys);
            return value_ptr + offs;
        }
        
        return nullptr;
    }
    
    // return number of records!
    size_t size() const
    {
        return bi_.size();
    }

    size_t get_mem_size() const
    {
        return bi_.get_mem_size();
    }
private:
    detail::BucketIndex const bi_;
    Key                 const mask_;
};




