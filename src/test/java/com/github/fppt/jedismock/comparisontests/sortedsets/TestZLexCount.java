package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(ComparisonBase.class)
public class TestZLexCount {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
        Map<String, Double> members = new HashMap<>();
        members.put("alpha", 0d);
        members.put("bar", 0d);
        members.put("cool", 0d);
        members.put("down", 0d);
        members.put("elephant", 0d);
        members.put("foo", 0d);
        members.put("great", 0d);
        members.put("hill", 0d);
        members.put("omega", 0d);
        long result = jedis.zadd(ZSET_KEY, members);
        assertEquals(9L, result);
    }

    @TestTemplate
    public void testZLexCountAllElements(Jedis jedis) {
        assertEquals(9, jedis.zlexcount(ZSET_KEY, "-", "+"));
    }

    @TestTemplate
    public void testZLexCountNoneElements(Jedis jedis) {
        assertEquals(0, jedis.zlexcount(ZSET_KEY, "+", "-"));
    }

    @TestTemplate
    public void testZLexCountStartPositiveInfinite(Jedis jedis) {
        assertEquals(0, jedis.zlexcount(ZSET_KEY, "+", "[c"));
    }

    @TestTemplate
    public void testZLexCountFinishNegativeInfinite(Jedis jedis) {
        assertEquals(0, jedis.zlexcount(ZSET_KEY, "[c", "-"));
    }

    @TestTemplate
    public void testZLexCountIncludeTwoBounds(Jedis jedis) {
        assertEquals(5, jedis.zlexcount(ZSET_KEY, "[bar", "[foo"));
    }

    @TestTemplate
    public void testZLexCountIncludeStartBound(Jedis jedis) {
        assertEquals(4, jedis.zlexcount(ZSET_KEY, "[bar", "(foo"));
    }

    @TestTemplate
    public void testZLexCountIncludeFinishBound(Jedis jedis) {
        assertEquals(4, jedis.zlexcount(ZSET_KEY, "(bar", "[foo"));
    }

    @TestTemplate
    public void testZLexCountNotIncludeBounds(Jedis jedis) {
        assertEquals(3, jedis.zlexcount(ZSET_KEY, "(bar", "(foo"));
    }

    @TestTemplate
    public void testZLexCountErrorMinValue(Jedis jedis) {
        assertThrows(RuntimeException.class,
                () -> jedis.zlexcount(ZSET_KEY, "bar", "(foo"));
    }

    @TestTemplate
    public void testZLexCountErrorMaxValue(Jedis jedis) {
        assertThrows(RuntimeException.class,
                () -> jedis.zlexcount(ZSET_KEY, "(bar", "foo"));
    }
}
