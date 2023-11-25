package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@ExtendWith(ComparisonBase.class)
public class TestZRem {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
    }

    @TestTemplate
    public void zRemRemovesKey(Jedis jedis) {
        Map<String, Double> members = new HashMap<>();
        members.put("myvalue1", 10d);
        members.put("myvalue2", 20d);

        long result = jedis.zadd(ZSET_KEY, members);

        assertEquals(2L, result);

        List<String> results = jedis.zrange(ZSET_KEY, 0, -1);

        assertEquals(2, results.size());
        assertEquals("myvalue1", results.get(0));
        assertEquals("myvalue2", results.get(1));

        result = jedis.zrem(ZSET_KEY, "myvalue1");

        assertEquals(1L, result);

        results = jedis.zrange(ZSET_KEY, 0, -1);

        assertEquals(1, results.size());
        assertEquals("myvalue2", results.get(0));
    }

    @TestTemplate
    public void testZRemLastValue(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1, "aaa");
        assertEquals(1, jedis.zrem(ZSET_KEY, "aaa"));
        assertFalse(jedis.exists(ZSET_KEY));
    }

    @TestTemplate
    public void testZRemKeyNotExist(Jedis jedis) {
        assertEquals(0, jedis.zrem(ZSET_KEY, "aaa"));
        assertFalse(jedis.exists(ZSET_KEY));
    }
}
