package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.*;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class TestZRevRange {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
        jedis.zadd(ZSET_KEY, 2, "aaaa");
        jedis.zadd(ZSET_KEY, 3, "bbbb");
        jedis.zadd(ZSET_KEY, 1, "cccc");
        jedis.zadd(ZSET_KEY, 3, "bcbb");
        jedis.zadd(ZSET_KEY, 3, "babb");
        assertEquals(5L, jedis.zcount(ZSET_KEY, Integer.MIN_VALUE, Integer.MAX_VALUE));
    }

    @TestTemplate
    public void whenUsingZrevrange_EnsureItReturnsEverythingInRightOrderWithPlusMinusMaxInteger(Jedis jedis) {
        assertEquals(Arrays.asList("bcbb", "bbbb", "babb", "aaaa", "cccc"), new ArrayList<>(jedis.zrevrange(ZSET_KEY, Integer.MIN_VALUE, Integer.MAX_VALUE)));
    }

    @TestTemplate
    public void whenUsingZrevrange_EnsureItReturnsListInRightOrderWithPositiveRange(Jedis jedis) {
        assertEquals(Arrays.asList("bbbb", "babb", "aaaa"), new ArrayList<>(jedis.zrevrange(ZSET_KEY, 1, 3)));
    }

    @TestTemplate
    public void whenUsingZrevrange_EnsureItReturnsListInRightOrderWithNegativeRange(Jedis jedis) {
        assertEquals(Arrays.asList("babb", "aaaa", "cccc"), new ArrayList<>(jedis.zrevrange(ZSET_KEY, -3, -1)));
    }

    @TestTemplate
    public void whenUsingZrevrange_EnsureItReturnsListInRightOrderWithNegativeStartAndPositiveEndRange(Jedis jedis) {
        assertEquals(Arrays.asList("bcbb", "bbbb", "babb"), new ArrayList<>(jedis.zrevrange(ZSET_KEY, -5, 2)));
    }

    @TestTemplate
    public void whenUsingZrevrange_EnsureItReturnsListInRightOrderWithPositiveStartAndNegativeEndRange(Jedis jedis) {
        assertEquals(Arrays.asList("bbbb", "babb", "aaaa", "cccc"), new ArrayList<>(jedis.zrevrange(ZSET_KEY, 1, -1)));
    }
}
