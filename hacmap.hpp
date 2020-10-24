#pragma once

#include "types.hpp"
#include "bitarray.hpp"
#include <type_traits>
#include <iostream>
#include <vector>
#include <algorithm>

namespace detail {

template<typename Key>
inline uint32_t binary_locate_compressed( Key const k, BitArrayAdapter const &keys, uint32_t u )
{
    uint32_t l = 0;
    
    while (l < u)
    {
        uint32_t i = (l + u) >> 1;

        uint32_t kval = keys[i];
        
        if (kval > k)
            u = i;
        else if (kval < k)
            l = i + 1;
        else {
            return i;
        }
    }
    
    return u;
}

} // namespace detail

template<typename Key, typename Value>
class EHCMapIndexer : private detail::KVCheck<Key, Value>
{
private:
    typedef std::pair<Key, Value>               kv_pair_t;
    typedef std::vector< kv_pair_t >            bucket_kv_array_t;
    typedef std::vector< bucket_kv_array_t >    bucket_array_t;
    typedef std::vector< kv_pair_t >            unsorted_records_list_t;
public:
    EHCMapIndexer( size_t reserve = 0 ) : kmask_(0) {
        unsorted_records_.reserve(reserve);
    }

    void add( std::pair<Key, Value> const &p )
    {
        add(p.first, p.second);
    }

    void add( Key k, Value v )
    {
        kmask_ |= k;
        unsorted_records_.emplace_back(k, v);
    }

    size_t size() const { return unsorted_records_.size(); }

    void clear()
    {
        unsorted_records_.clear();
        unsorted_records_.shrink_to_fit();
        kmask_ = 0;
    }

    std::vector<uint8_t> get_compacted( size_t const page_size = DEFAULT_PAGE_SIZE )
    {
        std::vector<uint8_t> buffer;
        utils::OStreamProxy os(buffer);
        compact_and_store(os, page_size);
        return buffer;
    }

private:
    void compact_and_store( utils::OStreamProxy &os, size_t const page_size )
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
        
        flush_buckets(os, buckets);
    }

    static void flush_bucket
    ( 
        utils::OStreamProxy     &os, 
        bucket_kv_array_t       &b, 
        uint32_t          const key_bits_store,
        uint32_t          const key_rshift_by
    )
    {
        if( !b.empty() )
        {
            // each bucket must sorted by key before get flushed
            std::sort(b.begin(), b.end());
            
            // store keys and values separatly
            // compress keys by storing only higher key part

            BitArrayWriter bwr(b.size() * key_bits_store);

            for( auto const &p : b )
            {
                bwr.AddBits(p.first >> key_rshift_by, key_bits_store);
                //std::cout << p.first << ':' << (p.first >> key_rshift_by) << ", ";
            }
            
            os.write(bwr.GetData(), bwr.GetCapacity());

            BitArrayAdapter barr(bwr.GetData(), key_bits_store, (1UL << key_bits_store) - 1);
            /*
            std::cout << "DUMP BUCKET KEYS: ";
            for( int i = 0; i < 20; ++i )
            {
                std::cout << barr[i] << ", ";
            }

            std::cout << std::endl;*/

            // store values uncompressed for now
            os.write_range(b.begin(), b.end(), []( kv_pair_t const &p ) { return p.second; });
        }
    }
    
    void flush_buckets( utils::OStreamProxy &os, bucket_array_t &buckets )
    {
        size_t const nbuckets = buckets.size();
        uint8_t nshift = utils::maxbits(nbuckets) - 1;
        uint32_t const key_bits_store0 = sizeof(Key) * 8 - nshift;
        uint32_t const key_rshift_by = sizeof(Key) * 8 - key_bits_store0;
        uint32_t const key_bits_store = 
            //key_bits_store0
            utils::maxbits(kmask_ >> key_rshift_by)
            ;

        BucketEntry be;
        uint64_t offs = sizeof(BucketEntry) * buckets.size();
        // write buckets index
        uint32_t bid = 0;
        for( auto const &b : buckets )
        {
            uint32_t nrec = b.size();
            be.nkeys = nrec;
            be.offset = offs;
            os << be;
            //std::cout << "BI: id=" << bid << " offs=" << offs << " nrec=" << nrec << std::endl;
            ++bid;
            if( nrec )
                offs += nrec * sizeof(Value)
                + detail::BucketIndex::get_kcompressed_size(nrec, key_bits_store);

        }

        // write each bucket
        for( auto &b : buckets )
            flush_bucket(os, b, key_bits_store, key_rshift_by);

        /*
        std::cout << "FOOTER WRITE: key_bits_store=" << key_bits_store
                  << " nshift=" << int(nshift)
                  << " key_rshift_by=" << key_rshift_by
                  << " nbuckets=" << nbuckets
                  << std::endl;*/

        // use footer, to keep alignment fine.
        // write footer;
        os << uint8_t(key_bits_store);
        // store only N from buckets = 2 ** (N - 1)
        nshift |= 0x80; // also set higher bit to signal footer reader get second byte too
        os << nshift; 
    }
private:
    unsorted_records_list_t     unsorted_records_;
    Key                         kmask_;
};
    

template<typename Key, typename Value>
class HACMapSearcher : private detail::KVCheck<Key, Value>
{
public:
    /* construct searcher from readable std::istream interface
     */
    HACMapSearcher( std::istream &is )
        : bi_(is)
        , mask_(bi_.get_mask())
        , bit_array_(nullptr, bi_.get_key_bits_store())
    {
        init();
    }
    
    /* usefull for tests and other */
    HACMapSearcher( EHCMapIndexer<Key, Value> &idx )
        : bi_(idx.get_compacted())
        , mask_(bi_.get_mask())
        , bit_array_(nullptr, bi_.get_key_bits_store())
    {
        init();
    }
    
    // unique key mode(get first equal key)
    // return pointer to found value or nullptr if not found!
    Value const* search( Key k ) const
    {
        auto const p = bi_.get_unpacked( k & mask_ );
        if( 0 == p.second )
            return nullptr;
        // get reduced key value to compare with prepared array
        Key kred = k >> key_rshift_by_;
        // update pointer to the selected bucket
        bit_array_.UpdatePtr(reinterpret_cast<uint64_t const*>(p.first));
        
        uint32_t offs = detail::binary_locate_compressed(kred, bit_array_, p.second);
        /*
        std::cout << "ATTEMP locate: k=" << k
                    << " bid=" << (k & mask_)
                    << " boffs=" << uint64_t(bi_.get(k & mask_).offset)
                    << " key_bits_store=" << key_bits_store
                    << " nshift=" << int(nshift)
                    << " kred=" << kred
                    << " kmask=0x" << std::hex << kmask_ << std::dec
                    << " nkeys=" << p.second
                    << " found offs=" << offs
                    << std::endl;

        std::cout << "DUMP BUCKET KEYS: ";
        for( int i = 0; i < 20; ++i )
        {
            std::cout << barr[i] << ", ";
        }

        std::cout << std::endl;*/
        
        if( offs < p.second )
        {
            // retrieve value by offset
            // we need to get start of the values
            // so calculate compressed keys size
            size_t keys_size = bi_.get_compressed_keys_size(p.second);
            uint8_t const *values_start = p.first + keys_size;
            return reinterpret_cast<Value const*>(values_start) + offs;
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
    void init()
    {
        uint8_t nshift = utils::maxbits(bi_.get_nbuckets()) - 1;
        uint32_t const key_bits_store0 = sizeof(Key) * 8 - nshift;
        key_rshift_by_ = sizeof(Key) * 8 - key_bits_store0;
    }
private:
    detail::BucketIndex const bi_;
    Key                 const mask_;
    mutable BitArrayAdapter   bit_array_;
    uint32_t                  key_rshift_by_;
};