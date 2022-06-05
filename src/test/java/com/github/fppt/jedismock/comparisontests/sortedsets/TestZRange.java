package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ZRangeParams;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(ComparisonBase.class)
public class TestZRange {

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
    public void whenUsingZrange_EnsureItReturnsEverythingInRightOrderWithPlusMinusMaxInteger(Jedis jedis) {
        assertEquals(Arrays.asList("cccc", "aaaa", "babb", "bbbb", "bcbb"), jedis.zrange(ZSET_KEY, Integer.MIN_VALUE, Integer.MAX_VALUE));
    }

    @TestTemplate
    public void whenUsingZrange_EnsureItReturnsListInRightOrderWithPositiveRange(Jedis jedis) {
        assertEquals(Arrays.asList("aaaa", "babb", "bbbb"), jedis.zrange(ZSET_KEY, 1, 3));
    }

    @TestTemplate
    public void whenUsingZrange_EnsureItReturnsListInRightOrderWithNegativeRange(Jedis jedis) {
        assertEquals(Arrays.asList("babb", "bbbb", "bcbb"), jedis.zrange(ZSET_KEY, -3, -1));
    }

    @TestTemplate
    public void whenUsingZrange_EnsureItReturnsListInRightOrderWithNegativeStartAndPositiveEndRange(Jedis jedis) {
        assertEquals(Arrays.asList("cccc", "aaaa", "babb"), jedis.zrange(ZSET_KEY, -5, 2));
    }

    @TestTemplate
    public void whenUsingZrange_EnsureItReturnsListInRightOrderWithPositiveStartAndNegativeEndRange(Jedis jedis) {
        assertEquals(Arrays.asList("aaaa", "babb", "bbbb", "bcbb"), jedis.zrange(ZSET_KEY, 1, -1));
    }

    @TestTemplate
    public void whenUsingZrange_EnsureItReturnsListInLexicographicOrderForSameScore(Jedis jedis) {
        jedis.zadd("foo", 42, "def");
        jedis.zadd("foo", 42, "abc");
        assertEquals(Arrays.asList("abc", "def"), jedis.zrange("foo", 0, -1));
        assertEquals(Arrays.asList("def", "abc"), jedis.zrange("foo", ZRangeParams.zrangeParams(0, -1).rev()));
    }

    @TestTemplate
    public void zRangeWorksSimilarToZRevRangeByScore(Jedis jedis) {
        jedis.zadd("foo", 1, "one");
        jedis.zadd("foo", 2, "two");
        jedis.zadd("foo", 3, "three");
        final List<String> list = jedis.zrange("foo", ZRangeParams.zrangeByScoreParams(3, 1).rev());
        assertEquals(Arrays.asList("three", "two", "one"), list);
    }
}
