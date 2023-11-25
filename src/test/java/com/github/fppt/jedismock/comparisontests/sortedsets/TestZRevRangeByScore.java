package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(ComparisonBase.class)
public class TestZRevRangeByScore {
    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
    }

    @TestTemplate
    void zRevRangeByScoreReturnsValues(Jedis jedis) {
        assertEquals(Arrays.asList("three", "two", "one"), jedis.zrevrangeByScore(ZSET_KEY, 3, 1));
    }

    @TestTemplate
    void sortElementsWithSameScoreLexicographically(Jedis jedis) {
        jedis.zadd("foo", 42, "abc");
        jedis.zadd("foo", 42, "def");
        final List<String> list = jedis.zrevrangeByScore("foo", 42, 42, 0, 1);
        assertEquals(Collections.singletonList("def"), list);
    }

    @TestTemplate
    void minusInfinity(Jedis jedis) {
        jedis.zadd("foo", 0, "abc");
        jedis.zadd("foo", 1, "def");
        final List<String> list = jedis.zrevrangeByScore("foo", "+inf", "-inf");
        assertEquals(Arrays.asList("def", "abc"), list);
    }

    @TestTemplate
    void outOfOrderBounds(Jedis jedis) {
        jedis.zadd("foo", 42, "bar");
        assertEquals(0, jedis.zrevrangeByScore("foo", 2, 5).size());
    }

    @TestTemplate
    void testZRevRangeByScoreInclusiveRange(Jedis jedis) {
        jedis.flushDB();
        jedis.zadd(ZSET_KEY, Double.NEGATIVE_INFINITY, "a");
        jedis.zadd(ZSET_KEY, 1, "b");
        jedis.zadd(ZSET_KEY, 2, "c");
        jedis.zadd(ZSET_KEY, 3, "d");
        jedis.zadd(ZSET_KEY, 4, "e");
        jedis.zadd(ZSET_KEY, 5, "f");
        jedis.zadd(ZSET_KEY, Double.POSITIVE_INFINITY, "g");
        assertEquals(Arrays.asList("c", "b", "a"), jedis.zrevrangeByScore(ZSET_KEY, "2", "-inf"));
        assertEquals(Arrays.asList("d", "c", "b"), jedis.zrevrangeByScore(ZSET_KEY, "3", "0"));
        assertEquals(Arrays.asList("f", "e", "d"), jedis.zrevrangeByScore(ZSET_KEY, "6", "3"));
        assertEquals(Arrays.asList("g", "f", "e"), jedis.zrevrangeByScore(ZSET_KEY, "+inf", "4"));
    }

    @TestTemplate
    void testZRevRangeByScoreInclusive(Jedis jedis) {
        jedis.flushDB();
        jedis.zadd(ZSET_KEY, 1, "b");
        jedis.zadd(ZSET_KEY, 2, "c");
        jedis.zadd(ZSET_KEY, 3, "d");
        jedis.zadd(ZSET_KEY, 4, "e");
        jedis.zadd(ZSET_KEY, 5, "f");
        assertEquals(Collections.emptyList(), jedis.zrevrangeByScore(ZSET_KEY, "+inf", "6"));
        assertEquals(Collections.emptyList(), jedis.zrevrangeByScore(ZSET_KEY, "-6", "-inf"));
    }

    @TestTemplate
    void testZRevRangeByScoreExclusiveRange(Jedis jedis) {
        jedis.flushDB();
        jedis.zadd(ZSET_KEY, Double.NEGATIVE_INFINITY, "a");
        jedis.zadd(ZSET_KEY, 1, "b");
        jedis.zadd(ZSET_KEY, 2, "c");
        jedis.zadd(ZSET_KEY, 3, "d");
        jedis.zadd(ZSET_KEY, 4, "e");
        jedis.zadd(ZSET_KEY, 5, "f");
        jedis.zadd(ZSET_KEY, Double.POSITIVE_INFINITY, "g");
        assertEquals(singletonList("b"), jedis.zrevrangeByScore(ZSET_KEY, "(2", "(-inf"));
        assertEquals(Arrays.asList("c", "b"), jedis.zrevrangeByScore(ZSET_KEY, "(3", "(0"));
        assertEquals(Arrays.asList("f", "e"), jedis.zrevrangeByScore(ZSET_KEY, "(6", "(3"));
        assertEquals(singletonList("f"), jedis.zrevrangeByScore(ZSET_KEY, "(+inf", "(4"));
    }

    @TestTemplate
    void testZRevRangeByScoreExclusive(Jedis jedis) {
        jedis.flushDB();
        jedis.zadd(ZSET_KEY, 1, "b");
        jedis.zadd(ZSET_KEY, 2, "c");
        jedis.zadd(ZSET_KEY, 3, "d");
        jedis.zadd(ZSET_KEY, 4, "e");
        jedis.zadd(ZSET_KEY, 5, "f");
        assertEquals(Collections.emptyList(), jedis.zrevrangeByScore(ZSET_KEY, "(+inf", "(6"));
        assertEquals(Collections.emptyList(), jedis.zrevrangeByScore(ZSET_KEY, "(-6", "(-inf"));
    }

}
