package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.resps.Tuple;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(ComparisonBase.class)
public class TestZadd {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void zaddAddsKey(Jedis jedis) {
        double score = 10;
        String value = "myvalue";

        long result = jedis.zadd(ZSET_KEY, score, value);

        assertEquals(1L, result);

        List<String> results = jedis.zrange(ZSET_KEY, 0, -1);

        assertEquals(1, results.size());
        assertEquals(value, results.get(0));
    }

    @TestTemplate
    public void zaddAddsKeys(Jedis jedis) {
        Map<String, Double> members = new HashMap<>();
        members.put("myvalue1", 10d);
        members.put("myvalue2", 20d);

        long result = jedis.zadd(ZSET_KEY, members);

        assertEquals(2L, result);

        List<String> results = jedis.zrange(ZSET_KEY, 0, -1);

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
        jedis.zadd(ZSET_KEY, 10d, "x");
        jedis.zadd(ZSET_KEY, 20d, "y");
        jedis.zadd(ZSET_KEY, 30d, "z");
        assertEquals(Arrays.asList("x", "y", "z"), jedis.zrange(ZSET_KEY, 0, -1));

        jedis.zadd(ZSET_KEY, 1d, "y");
        assertEquals(Arrays.asList("y", "x", "z"), jedis.zrange(ZSET_KEY, 0, -1));
    }

    @TestTemplate
    public void testZAddKeys(Jedis jedis) {
        Map<String, Double> members = new HashMap<>();
        members.put("a", 10d);
        members.put("b", 20d);
        members.put("c", 30d);

        long result = jedis.zadd(ZSET_KEY, members);
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY, 0, -1);

        assertEquals(result, results.size());
        assertEquals(new Tuple("a", 10.0), results.get(0));
        assertEquals(new Tuple("b", 20.0), results.get(1));
        assertEquals(new Tuple("c", 30.0), results.get(2));
    }

    @TestTemplate
    public void testZAddXXWithoutKey(Jedis jedis) {
        long result = jedis.zadd(ZSET_KEY, 10, "x", new ZAddParams().xx());

        assertEquals(0, result);
        assertEquals("none", jedis.type(ZSET_KEY));
    }

    @TestTemplate
    public void testZAddXXToExistKey(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 10, "x");
        long result = jedis.zadd(ZSET_KEY, 20, "y", new ZAddParams().xx());

        assertEquals(0, result);
        assertEquals(1, jedis.zcard(ZSET_KEY));
    }

    @TestTemplate
    public void testZAddGetNumberAddedElements(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 10, "x");
        Map<String, Double> members = new HashMap<>();
        members.put("x", 10d);
        members.put("y", 20d);
        members.put("z", 30d);
        long result = jedis.zadd(ZSET_KEY, members);

        assertEquals(2, result);
    }

    @TestTemplate
    public void testZAddXXUpdateExistingScores(Jedis jedis) {
        Map<String, Double> members1 = new HashMap<>();
        members1.put("x", 10d);
        members1.put("y", 20d);
        members1.put("z", 30d);
        jedis.zadd(ZSET_KEY, members1);

        Map<String, Double> members2 = new HashMap<>();
        members2.put("foo", 5d);
        members2.put("x", 11d);
        members2.put("y", 21d);
        members2.put("zap", 40d);
        jedis.zadd(ZSET_KEY, members2, new ZAddParams().xx());

        assertEquals(3, jedis.zcard(ZSET_KEY));
        assertEquals(11, jedis.zscore(ZSET_KEY, "x"));
        assertEquals(21, jedis.zscore(ZSET_KEY, "y"));
    }

    @TestTemplate
    public void testZAddXXandNX(Jedis jedis) {
        assertThrows(RuntimeException.class,
                () -> jedis.zadd(ZSET_KEY, 10, "x", new ZAddParams().xx().nx()));
    }

    @TestTemplate
    public void testZAddLTandGT(Jedis jedis) {
        assertThrows(RuntimeException.class,
                () -> jedis.zadd(ZSET_KEY, 10, "x", new ZAddParams().lt().gt()));
    }

    @TestTemplate
    public void testZAddNXWithNonExistingKey(Jedis jedis) {
        Map<String, Double> members = new HashMap<>();
        members.put("x", 10d);
        members.put("y", 20d);
        members.put("z", 30d);

        jedis.zadd(ZSET_KEY, members, new ZAddParams().nx());

        assertEquals(3, jedis.zcard(ZSET_KEY));
    }

    @TestTemplate
    public void testZAddNXOnlyAddNewElements(Jedis jedis) {
        Map<String, Double> members1 = new HashMap<>();
        members1.put("x", 10d);
        members1.put("y", 20d);
        members1.put("z", 30d);
        jedis.zadd(ZSET_KEY, members1);
        Map<String, Double> members2 = new HashMap<>();
        members2.put("x", 11d);
        members2.put("y", 21d);
        members2.put("a", 100d);
        members2.put("b", 200d);

        long result = jedis.zadd(ZSET_KEY, members2, new ZAddParams().nx());

        assertEquals(2, result);
        assertEquals(10, jedis.zscore(ZSET_KEY, "x"));
        assertEquals(20, jedis.zscore(ZSET_KEY, "y"));
        assertEquals(100, jedis.zscore(ZSET_KEY, "a"));
        assertEquals(200, jedis.zscore(ZSET_KEY, "b"));
    }

    @TestTemplate
    public void testZAddIncrLikeIncrBy(Jedis jedis) {
        Map<String, Double> members = new HashMap<>();
        members.put("x", 10d);
        members.put("y", 20d);
        members.put("z", 30d);
        jedis.zadd(ZSET_KEY, members);
        jedis.zaddIncr(ZSET_KEY, 15, "x", new ZAddParams());

        assertEquals(25, jedis.zscore(ZSET_KEY, "x"));
    }

    @TestTemplate
    public void testZAddIncrToNotExistKey(Jedis jedis) {
        assertEquals(15, jedis.zaddIncr(ZSET_KEY, 15, "x", new ZAddParams()));
    }

    @TestTemplate
    public void testZAddCHGetNumberChangedElements(Jedis jedis) {
        Map<String, Double> members1 = new HashMap<>();
        members1.put("x", 10d);
        members1.put("y", 20d);
        members1.put("z", 30d);
        jedis.zadd(ZSET_KEY, members1);

        Map<String, Double> members2 = new HashMap<>();
        members2.put("x", 11d);
        members2.put("y", 21d);
        members2.put("z", 30d);
        assertEquals(0, jedis.zadd(ZSET_KEY, members2));

        Map<String, Double> members3 = new HashMap<>();
        members3.put("x", 12d);
        members3.put("y", 22d);
        members3.put("z", 30d);
        assertEquals(2, jedis.zadd(ZSET_KEY, members3, new ZAddParams().ch()));
    }

    @TestTemplate
    public void testZAddGTXXCH(Jedis jedis) {
        Map<String, Double> members1 = new HashMap<>();
        members1.put("x", 10d);
        members1.put("y", 20d);
        members1.put("z", 30d);
        jedis.zadd(ZSET_KEY, members1);

        Map<String, Double> members2 = new HashMap<>();
        members2.put("foo", 5d);
        members2.put("x", 11d);
        members2.put("y", 21d);
        members2.put("z", 29d);
        assertEquals(2, jedis.zadd(ZSET_KEY, members2, new ZAddParams().gt().xx().ch()));

        assertEquals(3, jedis.zcard(ZSET_KEY));
        assertEquals(11, jedis.zscore(ZSET_KEY, "x"));
        assertEquals(21, jedis.zscore(ZSET_KEY, "y"));
        assertEquals(30, jedis.zscore(ZSET_KEY, "z"));
    }

    @TestTemplate
    public void testZAddIncrLTIfScoreNotUpdate(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 28, "x");

        assertNull(jedis.zaddIncr(ZSET_KEY, 1, "x", new ZAddParams().lt()));
        assertEquals(28, jedis.zscore(ZSET_KEY, "x"));
    }

    @TestTemplate
    public void testZAddIncrGTIfScoreNotUpdate(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 28, "x");

        assertNull(jedis.zaddIncr(ZSET_KEY, -1, "x", new ZAddParams().gt()));
        assertEquals(28, jedis.zscore(ZSET_KEY, "x"));
    }

}
