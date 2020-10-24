#pragma once

#include <vector>
#include <stdint.h>
#include <memory>
#include <fstream>

namespace utils {

enum DeleterType
{
    DELETER_TYPE_NONE           = 0,
    DELETER_TYPE_FREE           = 1, // using free()
    DELETER_TYPE_DELETEARRAY    = 2, // using delete[]
    DELETER_TYPE_UNMAP          = 3,
    DELETER_TYPE_PTR_MASK       = 0x3 
};

// Compact memory holder to the properly allocated data
// WARNING: initial ptr must be aligned at least to the 4 bytes!
class MemoryHolder
{
    MemoryHolder( size_t ptr, DeleterType dt, size_t mem_sz = 0 )
        : encoded_ptr_(ptr)
        , mem_size_(mem_sz)
    {
        if( encoded_ptr_ & DELETER_TYPE_PTR_MASK )
            throw std::runtime_error("[MemoryHolder] ptr not aligned!");

        encoded_ptr_ |= size_t(dt);
    }
public:
    MemoryHolder( MemoryHolder && o )
        : encoded_ptr_(o.encoded_ptr_)
        , mem_size_(o.mem_size_)
    {
        o.encoded_ptr_ = 0;
    }

    MemoryHolder& operator = ( MemoryHolder && o )
    {
        encoded_ptr_ = o.encoded_ptr_;
        mem_size_ = o.mem_size_;
        o.encoded_ptr_ = 0;
        return *this;
    }

    ~MemoryHolder()
    {
        if( get_ptr<void>() )
        {
            switch(encoded_ptr_ & DELETER_TYPE_PTR_MASK)
            {
                case DELETER_TYPE_FREE:
                    free(get_ptr<void>());
                    break;
                case DELETER_TYPE_DELETEARRAY:
                    delete[] get_ptr<uint8_t>();
                    break;
                case DELETER_TYPE_NONE:
                default:
                    break;
            }
        }
    }

    static MemoryHolder mk( std::unique_ptr<uint8_t[]> && p )
    {
        return MemoryHolder(size_t(p.release()), DELETER_TYPE_DELETEARRAY);
    }

    // sadly, but we need to make copy here
    static MemoryHolder mk( std::vector<uint8_t> && buffer )
    {
        size_t const mem_sz = buffer.size();
        uint8_t *p = new uint8_t[mem_sz];
        memcpy(p, buffer.data(), mem_sz);
        return MemoryHolder(size_t(p), DELETER_TYPE_DELETEARRAY, mem_sz);
    }

    static MemoryHolder mk( size_t mem_size_in_bytes )
    {
        return MemoryHolder(size_t(new uint8_t[mem_size_in_bytes]), DELETER_TYPE_DELETEARRAY, mem_size_in_bytes);
    }

    template<typename T>
    static MemoryHolder mk( T const *ptr, size_t mem_sz = 0 )
    {
        return MemoryHolder(size_t(ptr), DELETER_TYPE_NONE, mem_sz);
    }

    template<typename T>
    static MemoryHolder mk( T *ptr, size_t mem_sz = 0 )
    {
        return MemoryHolder(size_t(ptr), DELETER_TYPE_FREE, mem_sz);
    }

    template<typename T>
    T * get_ptr() const
    {
        return (T*)size_t(encoded_ptr_ & ~(DELETER_TYPE_PTR_MASK));
    }

    // return non-zero if you passed 3-rd argument at start
    size_t get_mem_size() const
    {
        return mem_size_;
    }
private:
    size_t    encoded_ptr_;
    // sadly, but we need to holding size for using munmap
    size_t    mem_size_;
};

class MemoryReader
{
public:
    /*
        init from std::istream(reads whole file into memory)
        */
    MemoryReader( std::istream &is )
        : mholder_(MemoryHolder::mk(file_size_from_current_to_end(is)))
        , mem_(mholder_.get_ptr<uint8_t>())
    {
        if( !is.read( mholder_.get_ptr<char>(), mholder_.get_mem_size() ).good() )
            throw std::runtime_error("[MemoryReader] failed to read whole stream");
    }

    size_t size() const { return mholder_.get_mem_size(); }

    void seek( size_t offs )
    {
        mem_ = mholder_.get_ptr<uint8_t>() + offs;
    }

    void seek_by( long by )
    {
        mem_ += by;
    }
    
    /*
        init from memory reference
        */
    MemoryReader( uint8_t const *mem, size_t mem_size )
        : mholder_(MemoryHolder::mk(mem, mem_size))
        , mem_(mholder_.get_ptr<uint8_t>())
        {}
    
    /*
        int from memory buffer
        */
    MemoryReader( std::vector<uint8_t> && buffer )
        : mholder_(MemoryHolder::mk(std::move(buffer)))
        , mem_(mholder_.get_ptr<uint8_t>())
    {}
    
    template<typename T>
    MemoryReader& operator >> ( T &out )
    {
        // TODO: is integral type check
        
        T const *pt = reinterpret_cast<T const*>(mem_);
        mem_ += sizeof(T);
        
        out = *pt;
        
        return *this;
    }

    size_t get_offset() const
    {
        return mem_ - mholder_.get_ptr<uint8_t>();
    }
    
    MemoryHolder&& get_ownership()
    {            
        return std::move(mholder_);
    }

    static size_t file_size_from_current_to_end( std::istream &is )
    {
        size_t const curr = is.tellg();
        is.seekg(0, std::ios::end);
        size_t const size = is.tellg();
        // return back stream state
        is.seekg (curr, std::ios::beg);
        is.clear();
        return size - curr;
    }
    
private:
    MemoryHolder                mholder_;
    uint8_t               const *mem_;
};


class OStreamProxy
{
public:
    OStreamProxy( std::ostream &os ) : os_(&os), buffer_(nullptr) {}
    OStreamProxy( std::vector<uint8_t> &buffer ) : os_(nullptr), buffer_(&buffer) {}
    
    template<typename T>
    OStreamProxy & operator << ( T const &v )
    { 
        static_assert( std::is_trivial<T>::value, "support only POD types!" );
        return write(&v, sizeof(v)); 
    }

    void prealloc( size_t sz ) 
    {
        if( buffer_ )
            buffer_->reserve(sz);
    }
    
    template<typename Iter, typename Func>
    void write_range( Iter beg, Iter end, Func f )
    {
        for( ; beg != end; ++ beg )
        {
            *this << f(*beg);
        }
    }

    size_t tellp() const
    {
        if(buffer_)
            return buffer_->size();
        

        return os_->tellp();
    }
    
    template<typename T>
    OStreamProxy& write( T const *data, size_t sz )
    {
        if (buffer_)
        {
            uint8_t const *beg = reinterpret_cast<uint8_t const*>(data);
            buffer_->insert(buffer_->end(), beg, beg + sz);
        }
        else
        {
            os_->write(reinterpret_cast<const char *>(data), sz);
        }
        
        return *this;
    }

private:
    std::ostream            *os_;
    std::vector<uint8_t>    *buffer_;
};
 
} // namespace utils