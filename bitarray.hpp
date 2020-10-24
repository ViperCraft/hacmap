#pragma once

#include <stdint.h>
#include <vector>

/* Classic bit-array for packing:
 * = Optimized for 64-bit machines.
 * = Use BMI special instructions for speedup.
 * = ReadOnly model specific optimization for speed.
 */

class BitArrayWriter
{
    static constexpr size_t nBits = sizeof(uint64_t) * 8;
public:
    BitArrayWriter( size_t capacity_in_bits )
    : data_(capacity_in_bits ? (1 + (capacity_in_bits - 1) / nBits) : 0)
    , last_bit_pos_(0)
    {}
    void AddBit( bool value );
    void AddBits( uint64_t value, uint64_t nbits );
    void AddBits( uint64_t const *values, size_t sz, uint64_t nbits );
    void SetBit( uint64_t pos, bool value );
    uint64_t GetPos() const { return last_bit_pos_; }
    // return block capacity in bytes
    size_t GetCapacity() const { return data_.size() * sizeof(uint64_t); }
    uint64_t GetBitCapacity() const { return data_.size() * uint64_t(nBits); }
    uint64_t const *GetData() const { return data_.data(); }
private:
    void Resize( uint64_t min_cap )
    {
        uint64_t min_sz = std::max(uint64_t(data_.size()), min_cap / nBits + 1);
        // simulate here 1.5 grown
        data_.resize((min_sz << 1) - (min_sz >> 1));
    }
private:
    std::vector<uint64_t>   data_;
    uint64_t                last_bit_pos_;
};

class BitArrayReader
{
    static constexpr size_t nBits = sizeof(uint64_t) * 8;
public:
    BitArrayReader( uint64_t const *data, size_t capacity = 0 )
        : data_(data)
#ifndef NDEBUG
        , capacity_(capacity)
#endif
        {}

    BitArrayReader( BitArrayWriter const &writer )
        : data_(writer.GetData())
#ifndef NDEBUG
        , capacity_(writer.GetBitCapacity())
#endif
        {}

    void UpdatePtr( uint64_t const *data ) { data_ = data; }

    bool GetBit( uint64_t pos ) const;
    uint64_t GetWord( uint64_t pos ) const;
    uint64_t GetBits( uint64_t pos, uint64_t mask ) const
    {
        return GetWord(pos) & mask;
    }
private:
    void CheckBounds( uint64_t pos ) const
    {
#ifndef NDEBUG
        if( capacity_ > 0 && pos >= capacity_ )
            throw std::out_of_range("[BitArrayReader] check bounds pos: " + std::to_string(pos));
#endif
    }
private:
    uint64_t    const *data_;
#ifndef NDEBUG
    size_t    const capacity_;
#endif
};

class BitArrayAdapter : public BitArrayReader
{
public:
    BitArrayAdapter( uint64_t const *data, size_t elem_width )
        : BitArrayReader(data)
        , elem_width_(elem_width)
        , emask_((1UL << elem_width) - 1)
        {}

    BitArrayAdapter( uint64_t const *data, size_t elem_width, uint64_t emask )
        : BitArrayReader(data)
        , elem_width_(elem_width)
        , emask_(emask)
        {}

    BitArrayAdapter( uint64_t const *data, size_t bit_capacity, size_t elem_width, uint64_t emask )
        : BitArrayReader(data, bit_capacity)
        , elem_width_(elem_width)
        , emask_(emask)
        {}
    
    uint64_t operator [] ( size_t offs ) const
    {
        return this->GetBits(offs * elem_width_, emask_);
    }
private:
    size_t const elem_width_;
    uint64_t const emask_;
};

inline void BitArrayWriter::AddBit( bool value )
{
    if( last_bit_pos_ == GetBitCapacity() )
        Resize(0);
    if( value )
        data_[last_bit_pos_ / nBits] |= 1UL << (last_bit_pos_ % nBits);
    ++last_bit_pos_;
}

inline void BitArrayWriter::SetBit( uint64_t pos, bool value )
{
    if( pos >= GetBitCapacity() )
        Resize(pos);

    if (value) 
        data_[pos / nBits] |= 1UL << (pos % nBits);
    else
        data_[pos / nBits] &= ~(1UL << (pos % nBits));
}

inline void BitArrayWriter::AddBits( uint64_t value, uint64_t nbits )
{
    uint64_t pos = last_bit_pos_;
    last_bit_pos_ += nbits;
    if( GetBitCapacity() < last_bit_pos_ )
        Resize(last_bit_pos_);
    
    for( uint64_t i = 0; i < nbits; ++i )
    {
        data_[pos / nBits] |= ((value >> i) & 1) << ( pos & 0x3f );
        ++pos;
    }
}

inline void BitArrayWriter::AddBits( uint64_t const *values, size_t sz, uint64_t nbits )
{
    uint64_t pos = last_bit_pos_;
    last_bit_pos_ += sz * nbits;
    if( GetBitCapacity() < last_bit_pos_ )
        Resize(last_bit_pos_);
    
    for( uint64_t const *end = values + sz; values != end; ++values )
    {
        uint64_t const value = *values;
        for( uint64_t i = 0; i < nbits; ++i )
        {
            data_[pos / nBits] |= ((value >> i) & 1) << ( pos & 0x3f );
            ++pos;
        }
    }
}

inline bool BitArrayReader::GetBit( uint64_t pos ) const
{
    CheckBounds(pos);
    return ((data_[pos / nBits] >> (pos % nBits)) & 1) != 0;
}

inline uint64_t BitArrayReader::GetWord( uint64_t pos ) const
{
    CheckBounds(pos);
    size_t offset = pos % nBits;
    size_t const bpos = pos / nBits;
    uint64_t out = (data_[bpos] >> offset);
    if (offset) 
    {
        out |= data_[bpos + 1] << (nBits - offset);
    }
    return out;
}