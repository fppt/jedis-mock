package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ZParams;
import redis.clients.jedis.resps.Tuple;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(ComparisonBase.class)
public class TestZInter {

    private static final String ZSET_KEY_1 = "myzset";
    private static final String ZSET_KEY_2 = "ztmp";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void testZInterNotExistKeyToNotExistDest(Jedis jedis) {
        assertEquals(Collections.emptySet(), jedis.zinter(new ZParams(), ZSET_KEY_1));
    }

    @TestTemplate
    public void testZInterWithEmptySet(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        assertEquals(Collections.emptySet(), jedis.zinterWithScores(new ZParams(), ZSET_KEY_1, ZSET_KEY_2));
    }

    @TestTemplate
    public void testZInterBaseInter(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "1");
        jedis.zadd(ZSET_KEY_1, 2, "2");
        jedis.zadd(ZSET_KEY_1, 3, "3");
        jedis.zadd(ZSET_KEY_2, 1, "1");
        jedis.zadd(ZSET_KEY_2, 3, "3");
        jedis.zadd(ZSET_KEY_2, 4, "4");
        Set<Tuple> results = jedis.zinterWithScores(new ZParams(), ZSET_KEY_1, ZSET_KEY_2);
        Set<Tuple> expected = new TreeSet<>();
        expected.add(new Tuple("1", 2.0));
        expected.add(new Tuple("3", 6.0));
        assertEquals(expected, results);
    }

    @TestTemplate
    public void testZInterWithWeights(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        jedis.zadd(ZSET_KEY_1, 3, "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        Set<Tuple> results = jedis.zinterWithScores(new ZParams().weights(2, 3), ZSET_KEY_1, ZSET_KEY_2);
        Set<Tuple> expected = new TreeSet<>();
        expected.add(new Tuple("b", 7.0));
        expected.add(new Tuple("c", 12.0));
        assertEquals(expected, results);
    }

    @TestTemplate
    public void testZInterWithAggregateMin(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        jedis.zadd(ZSET_KEY_1, 3, "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        Set<Tuple> results = jedis.zinterWithScores(
                new ZParams().aggregate(ZParams.Aggregate.valueOf("MIN")), ZSET_KEY_1, ZSET_KEY_2);
        Set<Tuple> expected = new TreeSet<>();
        expected.add(new Tuple("b", 1.0));
        expected.add(new Tuple("c", 2.0));
        assertEquals(expected, results);
    }

    @TestTemplate
    public void testZInterWithAggregateMax(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        jedis.zadd(ZSET_KEY_1, 3, "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        Set<Tuple> results = jedis.zinterWithScores(
                new ZParams().aggregate(ZParams.Aggregate.valueOf("MAX")), ZSET_KEY_1, ZSET_KEY_2);
        Set<Tuple> expected = new TreeSet<>();
        expected.add(new Tuple("b", 2.0));
        expected.add(new Tuple("c", 3.0));
        assertEquals(expected, results);
    }
}
