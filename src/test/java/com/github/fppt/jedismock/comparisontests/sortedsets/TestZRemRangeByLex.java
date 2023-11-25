package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class TestZRemRangeByLex {

    private final String key = "mykey";

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
        long result = jedis.zadd(key, members);
        assertEquals(9L, result);
    }

    @TestTemplate
    public void testZRemRangeByLexInclusiveRangeNegInf(Jedis jedis) {
        assertEquals(3, jedis.zremrangeByLex(key, "-", "[cool"));
        assertEquals(Arrays.asList("down", "elephant", "foo", "great", "hill", "omega"),
                jedis.zrange(key, 0, -1));
    }

    @TestTemplate
    public void testZRemRangeByLexInclusiveRangeInclude(Jedis jedis) {
        assertEquals(3, jedis.zremrangeByLex(key, "[bar", "[down"));
        assertEquals(Arrays.asList("alpha", "elephant", "foo", "great", "hill", "omega"),
                jedis.zrange(key, 0, -1));
    }

    @TestTemplate
    public void testZRemRangeByLexInclusiveRangePosInf(Jedis jedis) {
        assertEquals(3, jedis.zremrangeByLex(key, "[g", "+"));
        assertEquals(Arrays.asList("alpha", "bar", "cool", "down", "elephant", "foo"),
                jedis.zrange(key, 0, -1));
    }

    @TestTemplate
    public void testZRemRangeByLexExclusiveRangeNegInf(Jedis jedis) {
        assertEquals(2, jedis.zremrangeByLex(key, "-", "(cool"));
        assertEquals(Arrays.asList("cool", "down", "elephant", "foo", "great", "hill", "omega"),
                jedis.zrange(key, 0, -1));
    }

    @TestTemplate
    public void testZRemRangeByLexExclusiveRangeInclude(Jedis jedis) {
        assertEquals(1, jedis.zremrangeByLex(key, "(bar", "(down"));
        assertEquals(Arrays.asList("alpha", "bar", "down", "elephant", "foo", "great", "hill", "omega"),
                jedis.zrange(key, 0, -1));
    }

    @TestTemplate
    public void testZRemRangeByLexExclusiveRangePosInf(Jedis jedis) {
        assertEquals(2, jedis.zremrangeByLex(key, "(great", "+"));
        assertEquals(Arrays.asList("alpha", "bar", "cool", "down", "elephant", "foo", "great"),
                jedis.zrange(key, 0, -1));
    }

    @TestTemplate
    public void testZRemRangeByLexInclusiveAndExclusiveRangeNegInf(Jedis jedis) {
        assertEquals(0, jedis.zremrangeByLex(key, "-", "[aaaa"));
        assertEquals(Arrays.asList("alpha", "bar", "cool", "down", "elephant", "foo", "great", "hill", "omega"),
                jedis.zrange(key, 0, -1));
    }

    @TestTemplate
    public void testZRemRangeByLexInclusiveAndExclusiveRangeInclude(Jedis jedis) {
        assertEquals(0, jedis.zremrangeByLex(key, "(az", "(b"));
        assertEquals(Arrays.asList("alpha", "bar", "cool", "down", "elephant", "foo", "great", "hill", "omega"),
                jedis.zrange(key, 0, -1));
    }

    @TestTemplate
    public void testZRemRangeByLexInclusiveAndExclusiveRangePosInf(Jedis jedis) {
        assertEquals(0, jedis.zremrangeByLex(key, "(z", "+"));
        assertEquals(Arrays.asList("alpha", "bar", "cool", "down", "elephant", "foo", "great", "hill", "omega"),
                jedis.zrange(key, 0, -1));
    }

    @TestTemplate
    public void testZRemRangeByLexEmpty(Jedis jedis) {
        assertEquals(9, jedis.zremrangeByLex(key, "-", "+"));
        assertEquals(0, jedis.zcard(key));
        assertFalse(jedis.exists(key));
    }
}
