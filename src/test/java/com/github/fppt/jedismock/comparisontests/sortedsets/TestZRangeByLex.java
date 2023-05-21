package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(ComparisonBase.class)
public class TestZRangeByLex {

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
        List<String> results = jedis.zrangeByLex(key, "-", "+");
        assertEquals(Arrays.asList("aaa", "bbb", "ccc", "ddd"), results);
    }

    @TestTemplate
    void zrangebylexKeysCorrectOrderBounded(Jedis jedis) {
        List<String> results = jedis.zrangeByLex(key, "[bbb", "(ddd");
        assertEquals(Arrays.asList("bbb", "ccc"), results);
    }

    @TestTemplate
    public void zrevrangebylexKeysCorrectOrderUnbounded(Jedis jedis) {
        List<String> results = jedis.zrevrangeByLex(key, "+", "-");
        assertEquals(Arrays.asList("ddd", "ccc", "bbb", "aaa"), results);
    }

    @TestTemplate
    void zrevrangebylexKeysCorrectOrderBounded(Jedis jedis) {
        List<String> results = jedis.zrevrangeByLex(key, "[ddd", "(bbb");
        assertEquals(Arrays.asList("ddd", "ccc"), results);
    }


    @TestTemplate
    public void zrangebylexKeysThrowsOnIncorrectParameters(Jedis jedis) {
        assertThrows(JedisDataException.class, () -> jedis.zrangeByLex(key, "b", "[d"));
        assertThrows(JedisDataException.class, () -> jedis.zrevrangeByLex(key, "b", "[d"));
        assertThrows(JedisDataException.class, () -> jedis.zrangeByLex(key, "[b", "d"));
        assertThrows(JedisDataException.class, () -> jedis.zrevrangeByLex(key, "[b", "d"));
    }

    @TestTemplate
    public void zrangebylexLimit(Jedis jedis) {
        assertEquals(Collections.singletonList("aaa"),
                jedis.zrangeByLex(key, "[a", "(c", 0, 1));
    }

    @TestTemplate
    public void zrevrangebylexLimit(Jedis jedis) {
        assertEquals(Collections.singletonList("bbb"),
                jedis.zrevrangeByLex(key, "(c", "[a", 0, 1));
    }

    @TestTemplate
    public void zrangebylexNegativeLimit(Jedis jedis) {
        assertEquals(Arrays.asList("aaa", "bbb"),
                jedis.zrangeByLex(key, "[a", "(c", 0, -1));
    }
}
