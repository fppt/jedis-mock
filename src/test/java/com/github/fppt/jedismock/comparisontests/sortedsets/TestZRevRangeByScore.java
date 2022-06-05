package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.Arrays;

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
}
