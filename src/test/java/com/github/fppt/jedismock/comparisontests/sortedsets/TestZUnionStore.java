package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ZParams;
import redis.clients.jedis.resps.Tuple;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class TestZUnionStore {

    private static final String ZSET_KEY_1 = "myzset";
    private static final String ZSET_KEY_2 = "ztmp";
    private static final String ZSET_KEY_OUT = "zout";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void testZUnionStoreNotExistKeyToNotExistDest(Jedis jedis) {
        assertEquals(0, jedis.zunionstore(ZSET_KEY_OUT, ZSET_KEY_1));
        assertFalse(jedis.exists(ZSET_KEY_OUT));
    }

    @TestTemplate
    public void testZUnionStoreWithEmptySet(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        assertEquals(2, jedis.zunionstore(ZSET_KEY_OUT, ZSET_KEY_1, ZSET_KEY_2));
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertEquals(new Tuple("a", 1.0), results.get(0));
        assertEquals(new Tuple("b", 2.0), results.get(1));
    }

    @TestTemplate
    public void testZUnionStoreBaseUnion(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        jedis.zadd(ZSET_KEY_1, 3, "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        assertEquals(4, jedis.zunionstore(ZSET_KEY_OUT, ZSET_KEY_1, ZSET_KEY_2));
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertEquals(new Tuple("a", 1.0), results.get(0));
        assertEquals(new Tuple("b", 3.0), results.get(1));
        assertEquals(new Tuple("d", 3.0), results.get(2));
        assertEquals(new Tuple("c", 5.0), results.get(3));
    }

    @TestTemplate
    public void testZUnionStoreWithWeights(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        jedis.zadd(ZSET_KEY_1, 3, "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        assertEquals(4, jedis.zunionstore(ZSET_KEY_OUT, new ZParams().weights(2, 3), ZSET_KEY_1, ZSET_KEY_2));
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertEquals(new Tuple("a", 2.0), results.get(0));
        assertEquals(new Tuple("b", 7.0), results.get(1));
        assertEquals(new Tuple("d", 9.0), results.get(2));
        assertEquals(new Tuple("c", 12.0), results.get(3));
    }

    @TestTemplate
    public void testZUnionStoreWithRegularSetAndWeights(Jedis jedis) {
        jedis.sadd(ZSET_KEY_1, "a", "b", "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        assertEquals(4, jedis.zunionstore(ZSET_KEY_OUT, new ZParams().weights(2, 3), ZSET_KEY_1, ZSET_KEY_2));
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertEquals(new Tuple("a", 2.0), results.get(0));
        assertEquals(new Tuple("b", 5.0), results.get(1));
        assertEquals(new Tuple("c", 8.0), results.get(2));
        assertEquals(new Tuple("d", 9.0), results.get(3));
    }

    @TestTemplate
    public void testZUnionStoreWithAggregateMin(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        jedis.zadd(ZSET_KEY_1, 3, "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        assertEquals(4, jedis.zunionstore(ZSET_KEY_OUT,
                new ZParams().aggregate(ZParams.Aggregate.valueOf("MIN")), ZSET_KEY_1, ZSET_KEY_2));
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertEquals(new Tuple("a", 1.0), results.get(0));
        assertEquals(new Tuple("b", 1.0), results.get(1));
        assertEquals(new Tuple("c", 2.0), results.get(2));
        assertEquals(new Tuple("d", 3.0), results.get(3));
    }

    @TestTemplate
    public void testZUnionStoreWithAggregateMax(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        jedis.zadd(ZSET_KEY_1, 3, "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        assertEquals(4, jedis.zunionstore(ZSET_KEY_OUT,
                new ZParams().aggregate(ZParams.Aggregate.valueOf("MAX")), ZSET_KEY_1, ZSET_KEY_2));
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertEquals(new Tuple("a", 1.0), results.get(0));
        assertEquals(new Tuple("b", 2.0), results.get(1));
        assertEquals(new Tuple("c", 3.0), results.get(2));
        assertEquals(new Tuple("d", 3.0), results.get(3));
    }

    @TestTemplate
    public void testZUnionStoreWithAggregateSum(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        jedis.zadd(ZSET_KEY_1, 3, "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        assertEquals(4, jedis.zunionstore(ZSET_KEY_OUT,
                new ZParams().aggregate(ZParams.Aggregate.valueOf("SUM")), ZSET_KEY_1, ZSET_KEY_2));
        List<Tuple> results = Arrays.asList(new Tuple("a", 1.0),
                new Tuple("b", 3.0),
                new Tuple("d", 3.0),
                new Tuple("c", 5.0));
        assertEquals(results, jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1));
    }

    @TestTemplate
    public void testZUnionStoreWithInfScores(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, Double.POSITIVE_INFINITY, "a");
        jedis.zadd(ZSET_KEY_2, Double.POSITIVE_INFINITY, "a");
        jedis.zunionstore(ZSET_KEY_OUT, ZSET_KEY_1, ZSET_KEY_2);
        assertEquals(Double.POSITIVE_INFINITY, jedis.zscore(ZSET_KEY_OUT, "a"));

        jedis.zadd(ZSET_KEY_1, Double.NEGATIVE_INFINITY, "a");
        jedis.zadd(ZSET_KEY_2, Double.POSITIVE_INFINITY, "a");
        jedis.zunionstore(ZSET_KEY_OUT, ZSET_KEY_1, ZSET_KEY_2);
        assertEquals(0, jedis.zscore(ZSET_KEY_OUT, "a"));

        jedis.zadd(ZSET_KEY_1, Double.POSITIVE_INFINITY, "a");
        jedis.zadd(ZSET_KEY_2, Double.NEGATIVE_INFINITY, "a");
        jedis.zunionstore(ZSET_KEY_OUT, ZSET_KEY_1, ZSET_KEY_2);
        assertEquals(0, jedis.zscore(ZSET_KEY_OUT, "a"));

        jedis.zadd(ZSET_KEY_1, Double.NEGATIVE_INFINITY, "a");
        jedis.zadd(ZSET_KEY_2, Double.NEGATIVE_INFINITY, "a");
        jedis.zunionstore(ZSET_KEY_OUT, ZSET_KEY_1, ZSET_KEY_2);
        assertEquals(Double.NEGATIVE_INFINITY, jedis.zscore(ZSET_KEY_OUT, "a"));
    }

    @TestTemplate
    public void testZUnionStoreWithNanScores(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_2, 1, "a");
        assertThrows(RuntimeException.class,
                () -> jedis.zunionstore(ZSET_KEY_OUT,
                        new ZParams().weights(Double.NaN, Double.NaN), ZSET_KEY_1, ZSET_KEY_2));
    }

    @TestTemplate
    public void testZUnionStoreNotCreateNaNScores(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, Double.NEGATIVE_INFINITY, "a");

        jedis.zunionstore(ZSET_KEY_OUT, new ZParams().weights(0), ZSET_KEY_1);
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertEquals(new Tuple("a", 0.0), results.get(0));

        jedis.zunionstore(ZSET_KEY_OUT, new ZParams().weights(1), ZSET_KEY_1);
        results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertEquals(new Tuple("a", Double.NEGATIVE_INFINITY), results.get(0));

        jedis.zunionstore(ZSET_KEY_OUT, new ZParams().weights(-2), ZSET_KEY_1);
        results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertEquals(new Tuple("a", Double.POSITIVE_INFINITY), results.get(0));
    }
}
