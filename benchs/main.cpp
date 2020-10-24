#include <iostream>
#include <vector>
#include <algorithm>
#include <map>
#include <unordered_map>
#include <iomanip>
#include "timestamp.hpp"
#include "../hamap.hpp"
#include "../hacmap.hpp"
#include <nmmintrin.h>

static uint32_t loop_count = 1;

unsigned int MurmurHash2 (char const * key, unsigned int len)
{
  const unsigned int m = 0x5bd1e995;
  const unsigned int seed = 0;
  const int r = 24;

  unsigned int h = seed ^ len;

  const unsigned char * data = (const unsigned char *)key;
  unsigned int k;

  while (len >= 4)
  {
      k  = data[0];
      k |= data[1] << 8;
      k |= data[2] << 16;
      k |= data[3] << 24;

      k *= m;
      k ^= k >> r;
      k *= m;

      h *= m;
      h ^= k;

      data += 4;
      len -= 4;
  }

  switch (len)
  {
    case 3:
      h ^= data[2] << 16;
    case 2:
      h ^= data[1] << 8;
    case 1:
      h ^= data[0];
      h *= m;
  };

  h ^= h >> 13;
  h *= m;
  h ^= h >> 15;

  return h;
}

template<typename K>
K hash_key( K k )
{
    return k;
}

uint32_t hash_key( uint32_t k )
{
    return MurmurHash2((char const*)&k, sizeof(k));
}

template<typename K, typename V>
size_t get_memory_usage( std::unordered_map<K, V> const *m )
{
  size_t count = 0;
  for (unsigned i = 0; i < m->bucket_count(); ++i) {
    size_t bucket_size = m->bucket_size(i);
    if (bucket_size == 0) {
      count++;
    }
    else {
      count += bucket_size;
    }
  }

  size_t sum = m->size() * sizeof(typename std::unordered_map<K, V>::value_type);
  sum += count * (sizeof(size_t) + sizeof(void*));

  return sum;
}

template<typename K, typename V>
size_t get_memory_usage( HAMapSearcher<K, V> const *m )
{
    return m->get_mem_size();
}


template<typename K, typename V>
size_t get_memory_usage( HACMapSearcher<K, V> const *m )
{
    return m->get_mem_size();
}

template<typename K, typename V>
size_t get_memory_usage( std::vector<std::pair<K, V>> const *m )
{
    return m->size() * sizeof(typename std::vector<std::pair<K, V>>::value_type);
}

template<typename T>
size_t get_memory_usage( T const *m )
{
    return 0;
}


template<typename K, typename V>
static void generate_data_range( size_t from, size_t to, std::vector<std::pair<K, V>> &out )
{
    if( to < from )
        throw std::runtime_error("\"from\" greater then \"to\"");
    size_t const cnt = to - from;
    out.reserve(cnt);
    for( size_t i = from; i < to; ++i )
        out.push_back(std::make_pair(hash_key(i), i - from));
}

template<typename K, typename V>
struct KeyComparer
{
    bool operator () ( K a, K b ) const
    {
        return a < b;
    }
    
    bool operator () ( std::pair<K, V> const &p, K k ) const
    {
        return p.first < k;
    }
};

template<typename K, typename V>
static std::pair<K,V> const* binary_locate( K const k, std::pair<K, V> const *arr, size_t u )
{
    size_t l = 0;
    
    while (l < u)
    {
        size_t i = (l + u) >> 1;
        
        if (arr[i].first > k)
            u = i;
        else if (arr[i].first < k)
            l = i + 1;
        else {
            return arr + i;
        }
    }
    
    return nullptr;
}

template<typename K, typename V>
static uint32_t bench_func
( 
    size_t from, 
    size_t to, 
    std::vector<std::pair<K, V>> const &src, 
    std::vector<std::pair<K, V>> const *m 
)
{
    uint32_t cs = 0;
    
    for( size_t i = from; i < to; ++i )
    {
        K k = i;

        auto it = binary_locate(k, m->data(), m->size());
        
        if( it )
            cs += it->second;
    }
    
    return cs;
}

template<typename K, typename V>
static uint32_t bench_func
( 
    size_t from, 
    size_t to, 
    std::vector<std::pair<K, V>> const &src, 
    std::map<K, V> const *m 
)
{
    uint32_t cs = 0;
    
    for( size_t i = from; i < to; ++i )
    {
        K k = i;
        auto it = m->find(k);
        
        if( it != m->end() )
            cs += it->second;
    }
    
    return cs;
}

template<typename K, typename V>
static uint32_t bench_func
( 
    size_t from, 
    size_t to, 
    std::vector<std::pair<K, V>> const &src, 
    std::unordered_map<K, V> const *m
)
{
    uint32_t cs = 0;
    
    for( size_t i = from; i < to; ++i )
    {
        K k = i;
        auto it = m->find(k);
        
        if( it != m->end() )
            cs += it->second;
    }
    
    return cs;
}

template<typename K, typename V>
static uint32_t bench_func
( 
    size_t from, 
    size_t to, 
    std::vector<std::pair<K, V>> const &src, 
    HAMapSearcher<K, V> const *m
)
{
    uint32_t cs = 0;
    
    for( size_t i = from; i < to; ++i )
    {
        K k = i;
        
        V const *found = m->search(k);
        
        if( found )
            cs += *found;

    }
    
    return cs;
}

template<typename K, typename V>
static uint32_t bench_func
( 
    size_t from, 
    size_t to, 
    std::vector<std::pair<K, V>> const &src, 
    HACMapSearcher<K, V> const *m
)
{
    uint32_t cs = 0;
    
    for( size_t i = from; i < to; ++i )
    {
        K k = i;
        
        V const *found = m->search(k);
        
        if( found )
            cs += *found;

    }
    
    return cs;
}

template<typename K, typename V, typename MapT>
static void bench_impl
(
    size_t from, size_t to, std::vector<std::pair<K, V>> const &src,
    MapT const *m,
    char const *name
)
{
    std::cout << "--- bench started for " << name << " f=" << from << " t=" 
              << to << " kv_size=" << sizeof(std::pair<K, V>) 
              << " memory usage: " << get_memory_usage(m)
              << std::endl;
    Timestamp ts;
    
    uint32_t cs = 0;
    
    for( uint32_t i = 0; i < loop_count; ++i )
    {
        cs += bench_func(from, to, src, m);
    }
    
    auto elapsed = ts.elapsed_millis();
    
    std::cout << "+++ bench done for " << name << " elapsed = " << elapsed << " ms. cs = " << cs << std::endl;
}

template<typename K, typename V>
static void bench( size_t from, size_t to )
{
    std::vector<std::pair<K, V>> src;
    generate_data_range(from, to, src);

    std::cout   << "\n///////////// BENCH => " << src.size() << " kv pairs. "
                << sizeof(std::pair<K, V>) * src.size() << " data bytes." << std::endl;
    {
        bench_impl<K, V>(from, to, src, &src, "vec_binary_search");
    }
    /*
    {
        // make std::map from src
        std::map<K, V> map;
        for( auto const &p : src )
        {
            map.emplace(p);
        }
        bench_impl<K, V>(from, to, src, std::bind(bench_std_map<K, V>, _1, _2, _3, &map), "std_map");
    }
    */
    {
        // make std::map from src
        std::unordered_map<K, V> map;
        for( auto const &p : src )
        {
            map.emplace(p);
        }
        bench_impl<K, V>(from, to, src, &map, "std_umap");
    }
    
    {
        HAMapIndexer<K, V> map(src.size());
        for( auto const &p : src )
        {
            map.add(p);
        }
        
        HAMapSearcher<K, V> srch(map);
        map.clear();
        bench_impl<K, V>(from, to, src, &srch, "eh_umap");
    }
    
    /*
    {
        HAMapIndexer<K, V> map(src.size(), 512);
        for( auto const &p : src )
        {
            map.add(p);
        }
        
        HAMapSearcher<K, V> srch(map);
        map.clear();
        bench_impl<K, V>(from, to, src, &srch, "eh_umap512");
    }

    {
        HAMapIndexer<K, V> map(src.size(), 128);
        for( auto const &p : src )
        {
            map.add(p);
        }
        
        HAMapSearcher<K, V> srch(map);
        map.clear();
        bench_impl<K, V>(from, to, src, &srch, "eh_umap128");
    }

    */
    {
        EHCMapIndexer<K, V> map(src.size());
        for( auto const &p : src )
        {
            map.add(p);
        }
        
        HACMapSearcher<K, V> srch(map);
        map.clear();
        bench_impl<K, V>(from, to, src, &srch, "eh_umap_compr");
    }
}

int main( int argc, char *argv[] )
{
    loop_count = 1000;
    for( auto sz : {32, 64, 128, 256, 512, 1024} )
    {
        bench<uint32_t, uint32_t>(0, sz * 1024 * 4);
    }
    loop_count = 10;
    //for( auto sz : {16, 64, 128, 256, 512, 1024} )
    for( auto sz : {1024, 2048, 4096, 8192} )
    {
        bench<uint64_t, uint64_t>(0, sz * 1024 * 4);
    }
    return 0;
}
