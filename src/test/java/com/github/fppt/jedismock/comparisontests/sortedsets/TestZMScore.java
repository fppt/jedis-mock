package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(ComparisonBase.class)
public class TestZMScore {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void testZScoreNotExistKey(Jedis jedis) {
        List<Double> expected = new ArrayList<>();
        expected.add(null);
        assertEquals(expected, jedis.zmscore(ZSET_KEY, "a"));
    }

    @TestTemplate
    public void testZScoreNotExistMember(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1, "a");
        jedis.zadd(ZSET_KEY, 2, "b");
        List<Double> expected = new ArrayList<>();
        expected.add(1.0);
        expected.add(null);
        expected.add(2.0);
        assertEquals(expected, jedis.zmscore(ZSET_KEY, "a", "c", "b"));
    }

    @TestTemplate
    public void testZScoreOK(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 2, "a");
        List<Double> expected = new ArrayList<>();
        expected.add(2.0);
        assertEquals(expected, jedis.zmscore(ZSET_KEY, "a"));
    }
}
