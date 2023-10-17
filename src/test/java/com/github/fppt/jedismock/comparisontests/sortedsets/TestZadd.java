package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.resps.Tuple;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(ComparisonBase.class)
public class TestZadd {

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void zaddAddsKey(Jedis jedis) {
        String key = "mykey";
        double score = 10;
        String value = "myvalue";

        long result = jedis.zadd(key, score, value);

        assertEquals(1L, result);

        List<String> results = jedis.zrange(key, 0, -1);

        assertEquals(1, results.size());
        assertEquals(value, results.get(0));
    }

    @TestTemplate
    public void zaddAddsKeys(Jedis jedis) {
        String key = "mykey";
        Map<String, Double> members = new HashMap<>();
        members.put("myvalue1", 10d);
        members.put("myvalue2", 20d);

        long result = jedis.zadd(key, members);

        assertEquals(2L, result);

        List<String> results = jedis.zrange(key, 0, -1);

        assertEquals(2, results.size());
        assertEquals("myvalue1", results.get(0));
        assertEquals("myvalue2", results.get(1));
    }

    @TestTemplate
    public void testZaddNonUTF8binary(Jedis jedis) {
        byte[] msg = new byte[]{(byte) 0xbe};
        jedis.zadd("foo".getBytes(), 42, msg);
        byte[] newMsg = jedis.zrange("foo".getBytes(), 0, 0).get(0);
        assertArrayEquals(msg, newMsg);
    }

    @TestTemplate
    public void testBasicZAddAndScoreUpdate(Jedis jedis) {
        String key = "mykey";

        jedis.zadd(key, 10d, "x");
        jedis.zadd(key, 20d, "y");
        jedis.zadd(key, 30d, "z");
        assertEquals(Arrays.asList("x", "y", "z"), jedis.zrange(key, 0, -1));

        jedis.zadd(key, 1d, "y");
        assertEquals(Arrays.asList("y", "x", "z"), jedis.zrange(key, 0, -1));
    }

    @TestTemplate
    public void testZAddKeys(Jedis jedis) {

        String key = "mykey";
        Map<String, Double> members = new HashMap<>();
        members.put("a", 10d);
        members.put("b", 20d);
        members.put("c", 30d);

        long result = jedis.zadd(key, members);
        List<Tuple> results = jedis.zrangeWithScores(key, 0, -1);

        assertEquals(result, results.size());
        assertEquals(new Tuple("a", 10.0), results.get(0));
        assertEquals(new Tuple("b", 20.0), results.get(1));
        assertEquals(new Tuple("c", 30.0), results.get(2));
    }
}
