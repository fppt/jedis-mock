package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(ComparisonBase.class)
public class TestZRevRank {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
        jedis.zadd(ZSET_KEY, 2, "bbb");
        jedis.zadd(ZSET_KEY, 3, "ccc");
        jedis.zadd(ZSET_KEY, 1, "aaa");
        jedis.zadd(ZSET_KEY, 5, "yyy");
        jedis.zadd(ZSET_KEY, 4, "xxx");
    }

    @TestTemplate
    public void getFirstRank(Jedis jedis) {
        assertEquals(4, jedis.zrevrank(ZSET_KEY, "aaa"));
    }
}
