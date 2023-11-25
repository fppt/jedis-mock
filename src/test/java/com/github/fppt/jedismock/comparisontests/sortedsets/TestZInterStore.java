package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ZParams;
import redis.clients.jedis.resps.Tuple;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class TestZInterStore {

    private static final String ZSET_KEY_1 = "myzset";
    private static final String ZSET_KEY_2 = "ztmp";
    private static final String ZSET_KEY_OUT = "zout";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void testZInterStoreNotExistKeyToNotExistDest(Jedis jedis) {
        assertEquals(0, jedis.zinterstore(ZSET_KEY_OUT, ZSET_KEY_1));
        assertFalse(jedis.exists(ZSET_KEY_OUT));
    }

    @TestTemplate
    public void testZInterStoreWithEmptySet(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        assertEquals(0, jedis.zinterstore(ZSET_KEY_OUT, ZSET_KEY_1, ZSET_KEY_2));
        assertFalse(jedis.exists(ZSET_KEY_OUT));
    }

    @TestTemplate
    public void testZInterStoreBase(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        jedis.zadd(ZSET_KEY_1, 3, "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        assertEquals(2, jedis.zinterstore(ZSET_KEY_OUT, ZSET_KEY_1, ZSET_KEY_2));
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertEquals(new Tuple("b", 3.0), results.get(0));
        assertEquals(new Tuple("c", 5.0), results.get(1));
    }

    @TestTemplate
    public void testZInterStoreWithWeights(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        jedis.zadd(ZSET_KEY_1, 3, "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        assertEquals(2, jedis.zinterstore(ZSET_KEY_OUT, new ZParams().weights(2, 3), ZSET_KEY_1, ZSET_KEY_2));
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertEquals(new Tuple("b", 7.0), results.get(0));
        assertEquals(new Tuple("c", 12.0), results.get(1));
    }

    @TestTemplate
    public void testZInterStoreWithRegularSetAndWeights(Jedis jedis) {
        jedis.sadd(ZSET_KEY_1, "a", "b", "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        assertEquals(2, jedis.zinterstore(ZSET_KEY_OUT, new ZParams().weights(2, 3), ZSET_KEY_1, ZSET_KEY_2));
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertEquals(new Tuple("b", 5.0), results.get(0));
        assertEquals(new Tuple("c", 8.0), results.get(1));
    }

    @TestTemplate
    public void testZInterStoreWithAggregateMin(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        jedis.zadd(ZSET_KEY_1, 3, "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        assertEquals(2, jedis.zinterstore(ZSET_KEY_OUT,
                new ZParams().aggregate(ZParams.Aggregate.valueOf("MIN")), ZSET_KEY_1, ZSET_KEY_2));
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertEquals(new Tuple("b", 1.0), results.get(0));
        assertEquals(new Tuple("c", 2.0), results.get(1));
    }

    @TestTemplate
    public void testZInterStoreWithAggregateMax(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        jedis.zadd(ZSET_KEY_1, 3, "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        assertEquals(2, jedis.zinterstore(ZSET_KEY_OUT,
                new ZParams().aggregate(ZParams.Aggregate.valueOf("MAX")), ZSET_KEY_1, ZSET_KEY_2));
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertEquals(new Tuple("b", 2.0), results.get(0));
        assertEquals(new Tuple("c", 3.0), results.get(1));
    }

    @TestTemplate
    public void testZInterStoreWithInfScores(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, Double.POSITIVE_INFINITY, "a");
        jedis.zadd(ZSET_KEY_2, Double.POSITIVE_INFINITY, "a");
        jedis.zinterstore(ZSET_KEY_OUT, ZSET_KEY_1, ZSET_KEY_2);
        assertEquals(Double.POSITIVE_INFINITY, jedis.zscore(ZSET_KEY_OUT, "a"));

        jedis.zadd(ZSET_KEY_1, Double.NEGATIVE_INFINITY, "a");
        jedis.zadd(ZSET_KEY_2, Double.POSITIVE_INFINITY, "a");
        jedis.zinterstore(ZSET_KEY_OUT, ZSET_KEY_1, ZSET_KEY_2);
        assertEquals(0, jedis.zscore(ZSET_KEY_OUT, "a"));

        jedis.zadd(ZSET_KEY_1, Double.POSITIVE_INFINITY, "a");
        jedis.zadd(ZSET_KEY_2, Double.NEGATIVE_INFINITY, "a");
        jedis.zinterstore(ZSET_KEY_OUT, ZSET_KEY_1, ZSET_KEY_2);
        assertEquals(0, jedis.zscore(ZSET_KEY_OUT, "a"));

        jedis.zadd(ZSET_KEY_1, Double.NEGATIVE_INFINITY, "a");
        jedis.zadd(ZSET_KEY_2, Double.NEGATIVE_INFINITY, "a");
        jedis.zinterstore(ZSET_KEY_OUT, ZSET_KEY_1, ZSET_KEY_2);
        assertEquals(Double.NEGATIVE_INFINITY, jedis.zscore(ZSET_KEY_OUT, "a"));
    }

    @TestTemplate
    public void testZInterStoreWithNanScores(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_2, 1, "a");
        assertThrows(RuntimeException.class,
                () -> jedis.zinterstore(ZSET_KEY_OUT,
                        new ZParams().weights(Double.NaN, Double.NaN), ZSET_KEY_1, ZSET_KEY_2));
    }
}
