#include "../../hamap.hpp"
#include "../../hacmap.hpp"
#include "gtest/gtest.h"
#include <fstream>



// Tests for the HAMapSearcher class.
class HAMapSearcherTest : public testing::Test {
protected:
    // You can remove any or all of these functions if its body
    // is empty.
    HAMapSearcherTest() {
        // You can do set-up work for each test here.
    }

    virtual ~HAMapSearcherTest() {
        // You can do clean-up work that doesn't throw exceptions here.
    }

    // If the constructor and destructor are not enough for setting up
    // and cleaning up each test, you can define the following methods:

    virtual void SetUp() {
        // Code here will be called immediately after the constructor (right
        // before each test).
    }

    virtual void TearDown() {
        // Code here will be called immediately after each test (right
        // before the destructor).
    }
};

TEST_F(HAMapSearcherTest, Get) {
    HAMapIndexer<int, int> idx;
    idx.add(1, 1);
    idx.add(2, 2);
    idx.add(3, 3);
    HAMapSearcher<int, int> searcher(idx);
    EXPECT_EQ(searcher.search(1), &idx.get_bucket_arr(0)[0].second);
}

TEST_F(HAMapSearcherTest, Get_Empty) {
    HAMapIndexer<int, int> idx;
    HAMapSearcher<int, int> searcher(idx);
    EXPECT_EQ(searcher.search(1), nullptr);
}

TEST_F(HAMapSearcherTest, Get_NotExist) {
    HAMapIndexer<int, int> idx;
    idx.add(1, 1);
    idx.add(2, 2);
    idx.add(3, 3);
    HAMapSearcher<int, int> searcher(idx);
    EXPECT_EQ(searcher.search(4), nullptr);
}

TEST_F(HAMapSearcherTest, Get_Multiple) {
    HAMapIndexer<int, int> idx;
    idx.add(1, 1);
    idx.add(2, 2);
    idx.add(3, 3);
    idx.add(4, 4);
    idx.add(5, 5);
    HAMapSearcher<int, int> searcher(idx);
    EXPECT_EQ(searcher.search(1), &idx.get_bucket_arr(0)[0].second);
    EXPECT_EQ(searcher.search(2), &idx.get_bucket_arr(0)[1].second);
    EXPECT_EQ(searcher.search(3), &idx.get_bucket_arr(0)[2].second);
    EXPECT_EQ(searcher.search(4), &idx.get_bucket_arr(1)[0].second);
    EXPECT_EQ(searcher.search(5), &idx.get_bucket_arr(1)[1].second);
}

TEST_F(HAMapSearcherTest, Get_Multiple_Empty) {
    HAMapIndexer<int, int> idx;
    HAMapSearcher<int, int> searcher(idx);
    EXPECT_EQ(searcher.search(1), nullptr);
}

TEST_F(HAMapSearcherTest, Get_Multiple_NotExist) {
    HAMapIndexer<int, int> idx;
    idx.add(1, 1);
    idx.add(2, 2);
    idx.add(3, 3);
    idx.add(4, 4);
    idx.add(5, 5);
    HAMapSearcher<int, int> searcher(idx);
    EXPECT_EQ(searcher.search(6), nullptr);
}

TEST_F(HAMapSearcherTest, Get_Multiple_EdgeCases) {
    // Edge case: empty key value
    HAMapIndexer<int, int> idx;
    idx.add(1, 1);
    idx.add(2, 2);
    idx.add(3, 3);
    idx.add(4, 4);
    idx.add(5, 5);
    HAMapSearcher<int, int> searcher(idx);
    EXPECT_EQ(searcher.search(0), nullptr);
}

TEST(UtilsTest, TestIsTrue)
{
    uint32_t const count = 256;
    // make a test binary file
    {
        std::ofstream ofs("test.bin", std::ios::trunc | std::ios::binary);
        
        EXPECT_EQ(true, ofs.good());
        
        utils::OStreamProxy wr(ofs);
        
        for( uint32_t i = 0; i < count; ++i )
        {
            wr << uint64_t(i);
        }
        
        
        EXPECT_EQ(count * sizeof(uint64_t), ofs.tellp());
    }
    // read binary file
    {
        std::ifstream ifs("test.bin", std::ios::binary);
        
        EXPECT_EQ(true, ifs.good());
        
        utils::MemoryReader rdr(ifs);
        
        for( uint32_t i = 0; i < count; ++i )
        {
            uint64_t v;
            rdr >> v;
            EXPECT_EQ(uint64_t(i), v);
        }
    }
}

TEST(ETMapCreation, TestIsTrue)
{

    HAMapIndexer<uint32_t, uint32_t> indexer;
    uint32_t const from = 1200, to = 100500, count = to - from;
    for( uint32_t i = from; i < to; ++i )
        indexer.add(i, i + 600);
    
    EXPECT_EQ(count, indexer.size());
    
    // attempt to flush
    {
        std::ofstream ofs("test.trie", std::ios::trunc | std::ios::binary);
        
        EXPECT_EQ(true, ofs.good());
        
        utils::OStreamProxy prx(ofs);
        
        indexer.compact_and_store(prx, DEFAULT_PAGE_SIZE);
    }
    
    // attempt to load from istream
    
    std::ifstream ifs("test.trie", std::ios::binary);
    
    EXPECT_EQ(true, ifs.good());
    
    
    HAMapSearcher<uint32_t, uint32_t> searcher(ifs);
    
    for( uint32_t i = from; i < to; ++i )
    {
        auto const *v = searcher.search(i);
        ASSERT_NE(nullptr, v);
        EXPECT_EQ(i + 600, *v);
        
        // and not found scan!
        v = searcher.search(i + to);
        ASSERT_EQ(nullptr, v);
        
    }
}

TEST(EHMapCreationWithBuckets, TestIsTrue)
{
    
    uint32_t const from = 336, to = 123456, count = to - from;
    // try to create with known records count
    HAMapIndexer<uint64_t, uint32_t> indexer(count);
    for( uint32_t i = from; i < to; ++i )
        indexer.add(i, i + 2);
    
    EXPECT_EQ(count, indexer.size());
    
    // attempt to flush
    {
        std::ofstream ofs("test.trie", std::ios::trunc | std::ios::binary);
        
        EXPECT_EQ(true, ofs.good());
        
        utils::OStreamProxy prx(ofs);
        
        indexer.compact_and_store(prx, DEFAULT_PAGE_SIZE);
    }
    
    // attempt to load from istream
    
    std::ifstream ifs("test.trie", std::ios::binary);
    
    EXPECT_EQ(true, ifs.good());
    
    
    HAMapSearcher<uint64_t, uint32_t> searcher(ifs);
    
    for( uint32_t i = from; i < to; ++i )
    {
        auto const *v = searcher.search(i);
        ASSERT_NE(nullptr, v);
        EXPECT_EQ(i + 2, *v);
        
        // and not found scan!
        v = searcher.search(i + to);
        ASSERT_EQ(nullptr, v);
    }
}

TEST(EHMapCreationWithBucketsAndNOIO, TestIsTrue)
{
    uint32_t const from = 512, to = 100500, count = (to - from) / 2;
    // try to create with known records count
    HAMapIndexer<uint64_t, uint32_t> indexer(count);
    for( uint32_t i = from; i < to; ++i )
    {
        if( 0 == (i & 1) )
            indexer.add(i, i + 36);
    }
    
    EXPECT_EQ(count, indexer.size());
    
    // use direct construct of searcher from indexer
    
    HAMapSearcher<uint64_t, uint32_t> searcher(indexer);
    
    for( uint32_t i = from; i < to; ++i )
    {
        auto const *v = searcher.search(i);
        if( 0 == (i & 1) )
        {
            ASSERT_NE(nullptr, v);
            EXPECT_EQ(i + 36, *v);
        }
        else
        {
            ASSERT_EQ(nullptr, v);
        }
        
        // and not found scan!
        v = searcher.search(i + to);
        ASSERT_EQ(nullptr, v);
    }
}

TEST(EHCMapCreationWithBucketsAndNOIO, TestIsTrue)
{
    uint32_t const from = 335, to = 100500, count = (to - from) / 2;
    // try to create with known records count
    EHCMapIndexer<uint32_t, uint32_t> indexer;
    for( uint32_t i = from; i < to; ++i )
    {
        if( 0 == (i & 1) )
            indexer.add(i, i + 37);
    }
    
    EXPECT_EQ(count, indexer.size());
    
    // use direct construct of searcher from indexer
    
    HACMapSearcher<uint32_t, uint32_t> searcher(indexer);
    
    for( uint32_t i = from; i < to; ++i )
    {
        auto const *v = searcher.search(i);
        if( 0 == (i & 1) )
        {
            ASSERT_NE(nullptr, v);
            EXPECT_EQ(i + 37, *v);
        }
        else
        {
            ASSERT_EQ(nullptr, v);
        }
        
        // and not found scan!
        v = searcher.search(i + to);
        ASSERT_EQ(nullptr, v);
    }
}

template<typename K, typename V>
static void check_range( size_t from, size_t to )
{
    EXPECT_LE(from, to);

    EHCMapIndexer<K, V> map(to - from);
    HAMapIndexer<K, V> chk_map(to - from);

    for( size_t i = from; i < to; ++i )
    {
        map.add(i, i + 117);
        chk_map.add(i, i + 117);
    }
    
    HACMapSearcher<K, V> srch(map);
    HAMapSearcher<K, V> chk_srch(chk_map);

    for( size_t i = from; i < to; ++i )
    {
        K k = i;

        V const *found0 = chk_srch.search(k);

        V const *found1 = srch.search(k);

        ASSERT_NE(nullptr, found0);
        ASSERT_NE(nullptr, found1);

        ASSERT_EQ(*found0, *found1);
    }
}


TEST(ComprVsOrdinal, TestIsTrue)
{
    check_range<uint32_t, uint64_t>(0, 10001);
    check_range<uint64_t, uint64_t>(10001, 100003);
    check_range<uint64_t, uint32_t>(111, 88774);
}

template <typename T>
class EHMapTest : public ::testing::Test {
protected:
    HAMapIndexer<uint64_t, T> mk_indexer(uint32_t count) {
        return HAMapIndexer<uint64_t, T>(count, DEFAULT_PAGE_SIZE);
    }
    HAMapSearcher<uint64_t, T> mk_searcher(HAMapIndexer<uint64_t, T> &idx) {
        return HAMapSearcher<uint64_t, T>(idx, DEFAULT_PAGE_SIZE);
    }
    size_t get_value_size() const {
        return sizeof(T);
    }
};

using PrimitiveTypes = ::testing::Types<uint16_t, uint32_t, uint64_t>;

TYPED_TEST_SUITE(EHMapTest, PrimitiveTypes);

TYPED_TEST(EHMapTest, EHMapCreationWithBucketsAndNOIO)
{
    uint32_t const from = 512, to = this->get_value_size() > 2 ? 100500 : 40000, count = (to - from) / 2;
    // try to create with known records count
    auto indexer = this->mk_indexer(count);
    for( uint32_t i = from; i < to; ++i )
    {
        if( 0 == (i & 1) )
            indexer.add(i, i + 36);
    }

    EXPECT_EQ(count, indexer.size());

    // use direct construct of searcher from indexer

    auto searcher = this->mk_searcher(indexer);

    for( uint32_t i = from; i < to; ++i )
    {
        auto const *v = searcher.search(i);
        if( 0 == (i & 1) )
        {
            ASSERT_NE(nullptr, v);
            EXPECT_EQ(i + 36, *v);
        }
        else
        {
            ASSERT_EQ(nullptr, v);
        }

        // and not found scan!
        v = searcher.search(i + to);
        ASSERT_EQ(nullptr, v);
    }
}