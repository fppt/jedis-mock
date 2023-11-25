package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.resps.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class SortedSetOperationsTest {

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void zcardEmptyKey(Jedis jedis) {
        String key = "mykey";

        long result = jedis.zcard(key);

        assertEquals(0L, result);
    }

    @TestTemplate
    public void zcardReturnsCount(Jedis jedis) {
        String key = "mykey";
        Map<String, Double> members = new HashMap<>();
        members.put("myvalue1", 10d);
        members.put("myvalue2", 20d);

        jedis.zadd(key, members);

        long result = jedis.zcard(key);

        assertEquals(2L, result);
    }

    @TestTemplate
    public void zrangeKeysCorrectOrder(Jedis jedis) {
        String key = "mykey";
        Map<String, Double> members = new HashMap<>();
        members.put("myvalue2", 10d);
        members.put("myvalue4", 20d);
        members.put("myvalue3", 15d);
        members.put("myvalue1", 9d);

        long result = jedis.zadd(key, members);

        assertEquals(4L, result);

        List<String> results = jedis.zrange(key, 0, -1);

        assertEquals(4, results.size());
        assertEquals("myvalue1", results.get(0));
        assertEquals("myvalue2", results.get(1));
        assertEquals("myvalue3", results.get(2));
        assertEquals("myvalue4", results.get(3));
    }

    @TestTemplate
    public void zrangeIndexOutOfRange(Jedis jedis) {
        String key = "mykey";
        Map<String, Double> members = new HashMap<>();
        members.put("myvalue2", 10d);
        members.put("myvalue4", 20d);
        members.put("myvalue3", 15d);
        members.put("myvalue1", 9d);

        long result = jedis.zadd(key, members);

        assertEquals(4L, result);

        List<String> results = jedis.zrange(key, 0, -6);

        assertEquals(0, results.size());
    }

    @TestTemplate
    public void zrangeWithScores(Jedis jedis) {
        String key = "mykey";
        Map<String, Double> members = new HashMap<>();
        members.put("myvalue2", 10d);
        members.put("myvalue4", 20d);
        members.put("myvalue3", 15d);
        members.put("myvalue1", 9d);

        long result = jedis.zadd(key, members);

        assertEquals(4L, result);

        List<Tuple> results = jedis.zrangeWithScores(key, 0, -1);

        assertEquals(4, results.size());
        assertEquals("myvalue1", results.get(0).getElement());
        assertEquals(9d, results.get(0).getScore(), 0);
        assertEquals("myvalue2", results.get(1).getElement());
        assertEquals(10d, results.get(1).getScore(), 0);
        assertEquals("myvalue3", results.get(2).getElement());
        assertEquals(15d, results.get(2).getScore(), 0);
        assertEquals("myvalue4", results.get(3).getElement());
        assertEquals(20d, results.get(3).getScore(), 0);
    }

    @TestTemplate
    public void zscore(Jedis jedis) {
        String key = "a_key";
        Map<String, Double> members = new HashMap<>();
        members.put("aaa", 0d);
        members.put("bbb", 1d);
        members.put("ddd", 1d);

        long result = jedis.zadd(key, members);
        assertEquals(3L, result);

        assertEquals(0d, jedis.zscore(key, "aaa"));
        assertEquals(1d, jedis.zscore(key, "bbb"));
        assertEquals(1d, jedis.zscore(key, "ddd"));
        assertNull(jedis.zscore(key, "ccc"));
    }

    @TestTemplate
    public void testGetOperation(Jedis jedis) {
        String key = "a_key";
        Map<String, Double> members = new HashMap<>();
        members.put("aaa", 0d);
        members.put("bbb", 1d);
        members.put("ddd", 1d);
        jedis.zadd(key, members);
        assertThrows(JedisDataException.class, () -> jedis.get("a_key"));
    }
}
