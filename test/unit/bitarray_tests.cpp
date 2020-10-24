#include "gtest/gtest.h"
#include "../../bitarray.hpp"

TEST(BitArrayBasic, TestIsTrue)
{
    BitArrayWriter wr(100);
    // fill odd bits
    for( int i = 0; i < 100; ++i )
    {
        wr.AddBit( (i & 1) != 0 );
    }

    BitArrayReader rdr(wr);

    for( int i = 0; i < 100; ++i )
    {
        if( (i & 1) )
            EXPECT_EQ(true, rdr.GetBit(i));
        else
            EXPECT_EQ(false, rdr.GetBit(i));
    }

    for( int i = 0; i < 299; ++i )
    {
        wr.SetBit( i, i < 100 ? false : true );
    }

    {
        BitArrayReader rdr(wr);

        for( int i = 0; i < 299; ++i )
        {
            if( i < 100 )
                EXPECT_EQ(false, rdr.GetBit(i));
            else
                EXPECT_EQ(true, rdr.GetBit(i));
        }
    }
}

TEST(BitArrayMultiBits, TestIsTrue)
{
    BitArrayWriter wr(100);
    uint64_t const v0 = 0x4545, v1 = 0x131313;
    for( int i = 0; i < 100; ++i )
    {
        wr.AddBits( i & 1 ? v0 : v1, i & 1 ? 13 : 17 );
    }

    EXPECT_EQ(uint64_t(50 * 13 + 50 * 17), wr.GetPos());


    BitArrayReader rdr(wr);

    uint64_t pos = 0;
    for( int i = 0; i < 100; ++i )
    {
        uint32_t nbits = i & 1 ? 13 : 17;
        uint64_t mask = (1UL << nbits) - 1;
        uint64_t v = rdr.GetBits(pos, mask);
        EXPECT_EQ( (i & 1 ? v0 : v1) & mask, v );
        pos += nbits;
    }
}

TEST(CapacityGranularity, TestIsTrue)
{
    for( auto wcap : { 8, 16, 17, 19, 255 } )
    {
        BitArrayWriter wr(wcap * 64);
        for( size_t i = 0; i < wcap * 64; ++i )
            wr.AddBit(true);
        
        EXPECT_EQ(wcap * 64U, wr.GetPos());
        EXPECT_EQ(wcap * 8U, wr.GetCapacity());
    }
}