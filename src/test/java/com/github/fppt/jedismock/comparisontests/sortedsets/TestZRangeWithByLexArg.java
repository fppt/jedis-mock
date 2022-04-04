package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.ZRangeParams;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(ComparisonBase.class)
public class TestZRangeWithByLexArg {

    private final String key = "mykey";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
        Map<String, Double> members = new HashMap<>();
        members.put("bbb", 0d);
        members.put("ddd", 0d);
        members.put("ccc", 0d);
        members.put("aaa", 0d);
        long result = jedis.zadd(key, members);
        assertEquals(4L, result);
    }

    @TestTemplate
    public void zrangebylexKeysCorrectOrderUnbounded(Jedis jedis) {
        List<String> results = new ArrayList<>(jedis.zrange(key, ZRangeParams.zrangeByLexParams("-", "+")));
        assertEquals(Arrays.asList("aaa", "bbb", "ccc", "ddd"), results);
    }

    @TestTemplate
    void zrangebylexKeysCorrectOrderBounded(Jedis jedis) {
        List<String> results = new ArrayList<>(jedis.zrange(key, ZRangeParams.zrangeByLexParams("[bbb", "(ddd")));
        assertEquals(Arrays.asList("bbb", "ccc"), results);
    }

    @TestTemplate
    public void zrevrangebylexKeysCorrectOrderUnbounded(Jedis jedis) {
        List<String> results = new ArrayList<>(jedis.zrange(key, ZRangeParams.zrangeByLexParams("+", "-").rev()));
        assertEquals(Arrays.asList("ddd", "ccc", "bbb", "aaa"), results);
    }

    @TestTemplate
    void zrevrangebylexKeysCorrectOrderBounded(Jedis jedis) {
        List<String> results = new ArrayList<>(jedis.zrange(key, ZRangeParams.zrangeByLexParams("[ddd", "(bbb").rev()));
        assertEquals(Arrays.asList("ddd", "ccc"), results);
    }

    @TestTemplate
    public void zrangebylexKeysThrowsOnIncorrectParameters(Jedis jedis) {
        assertThrows(JedisDataException.class, () -> jedis.zrange(key, ZRangeParams.zrangeByLexParams("b", "[d")));
        assertThrows(JedisDataException.class, () -> jedis.zrange(key, ZRangeParams.zrangeByLexParams("b", "[d").rev()));
        assertThrows(JedisDataException.class, () -> jedis.zrange(key, ZRangeParams.zrangeByLexParams("[b", "d")));
        assertThrows(JedisDataException.class, () -> jedis.zrange(key, ZRangeParams.zrangeByLexParams("[b", "d").rev()));
    }
}
