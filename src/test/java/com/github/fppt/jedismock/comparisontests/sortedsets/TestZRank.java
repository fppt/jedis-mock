package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(ComparisonBase.class)
public class TestZRank {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
    }

    @TestTemplate
    public void testZRankGetFirstRank(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 2, "bbb");
        jedis.zadd(ZSET_KEY, 3, "ccc");
        jedis.zadd(ZSET_KEY, 1, "aaa");
        jedis.zadd(ZSET_KEY, 5, "yyy");
        jedis.zadd(ZSET_KEY, 4, "xxx");

        assertEquals(0, jedis.zrank(ZSET_KEY, "aaa"));
    }

    @TestTemplate
    public void testZRankGetAllRanks(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 10, "x");
        jedis.zadd(ZSET_KEY, 20, "y");
        jedis.zadd(ZSET_KEY, 30, "z");

        assertEquals(0, jedis.zrank(ZSET_KEY, "x"));
        assertEquals(1, jedis.zrank(ZSET_KEY, "y"));
        assertEquals(2, jedis.zrank(ZSET_KEY, "z"));

        assertNull(jedis.zrank(ZSET_KEY, "foo"));
    }
}
