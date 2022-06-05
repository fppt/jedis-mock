package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
}
