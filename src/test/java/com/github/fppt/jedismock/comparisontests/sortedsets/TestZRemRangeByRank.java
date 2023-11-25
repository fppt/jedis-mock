package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class TestZRemRangeByRank {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void clearKey(Jedis jedis) {
        jedis.flushDB();
        Map<String, Double> members = new HashMap<>();
        members.put("a", 1d);
        members.put("b", 2d);
        members.put("c", 3d);
        members.put("d", 4d);
        members.put("e", 5d);
        long result = jedis.zadd(ZSET_KEY, members);
        assertEquals(5L, result);
    }

    @TestTemplate
    public void testZRemRangeByRankInnerRange(Jedis jedis) {
        assertEquals(3, jedis.zremrangeByRank(ZSET_KEY, 1, 3));
        assertEquals(Arrays.asList("a", "e"), jedis.zrange(ZSET_KEY, 0, -1));
    }

    @TestTemplate
    public void testZRemRangeByRankStartUnderflow(Jedis jedis) {
        assertEquals(1, jedis.zremrangeByRank(ZSET_KEY, -10, 0));
        assertEquals(Arrays.asList("b", "c", "d", "e"), jedis.zrange(ZSET_KEY, 0, -1));
    }

    @TestTemplate
    public void testZRemRangeByRankStartOverflow(Jedis jedis) {
        assertEquals(0, jedis.zremrangeByRank(ZSET_KEY, 10, -1));
        assertEquals(Arrays.asList("a", "b", "c", "d", "e"), jedis.zrange(ZSET_KEY, 0, -1));
    }

    @TestTemplate
    public void testZRemRangeByRankEndUnderflow(Jedis jedis) {
        assertEquals(0, jedis.zremrangeByRank(ZSET_KEY, 0, -10));
        assertEquals(Arrays.asList("a", "b", "c", "d", "e"), jedis.zrange(ZSET_KEY, 0, -1));
    }

    @TestTemplate
    public void testZRemRangeByRankEndOverflow(Jedis jedis) {
        assertEquals(5, jedis.zremrangeByRank(ZSET_KEY, 0, 10));
        assertEquals(Collections.emptyList(), jedis.zrange(ZSET_KEY, 0, -1));
    }

    @TestTemplate
    public void testZRemRangeByRankDeleteWhenEmpty(Jedis jedis) {
        assertEquals(5, jedis.zremrangeByRank(ZSET_KEY, 0, 4));
        assertFalse(jedis.exists(ZSET_KEY));
    }
}
