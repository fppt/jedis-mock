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

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class TestZUnion {

    private static final String ZSET_KEY_1 = "myzset";
    private static final String ZSET_KEY_2 = "ztmp";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void testZUnionNotExistKeyToNotExistDest(Jedis jedis) {
        assertEquals(Collections.emptySet(), jedis.zunion(new ZParams(), ZSET_KEY_1));
    }

    @TestTemplate
    public void testZUnionWithEmptySet(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        Set<Tuple> results = jedis.zunionWithScores(new ZParams(), ZSET_KEY_1, ZSET_KEY_2);

        assertTrue(results.contains(new Tuple("a", 1.0)));
        assertTrue(results.contains(new Tuple("b", 2.0)));
    }

    @TestTemplate
    public void testZUnionBaseUnion(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "1");
        jedis.zadd(ZSET_KEY_1, 2, "2");
        jedis.zadd(ZSET_KEY_1, 3, "3");
        jedis.zadd(ZSET_KEY_2, 1, "1");
        jedis.zadd(ZSET_KEY_2, 3, "3");
        jedis.zadd(ZSET_KEY_2, 4, "4");
        Set<Tuple> results = jedis.zunionWithScores(new ZParams(), ZSET_KEY_1, ZSET_KEY_2);
        Set<Tuple> expected = new TreeSet<>();
        expected.add(new Tuple("1", 2.0));
        expected.add(new Tuple("2", 2.0));
        expected.add(new Tuple("4", 4.0));
        expected.add(new Tuple("3", 6.0));
        assertEquals(expected, results);
    }

    @TestTemplate
    public void testZUnionWithWeights(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        jedis.zadd(ZSET_KEY_1, 3, "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        Set<Tuple> results = jedis.zunionWithScores(new ZParams().weights(2, 3), ZSET_KEY_1, ZSET_KEY_2);
        Set<Tuple> expected = new TreeSet<>();
        expected.add(new Tuple("a", 2.0));
        expected.add(new Tuple("b", 7.0));
        expected.add(new Tuple("d", 9.0));
        expected.add(new Tuple("c", 12.0));
        assertEquals(expected, results);
    }

    @TestTemplate
    public void testZUnionWithAggregateMin(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        jedis.zadd(ZSET_KEY_1, 3, "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        Set<Tuple> results = jedis.zunionWithScores(
                new ZParams().aggregate(ZParams.Aggregate.valueOf("MIN")), ZSET_KEY_1, ZSET_KEY_2);
        Set<Tuple> expected = new TreeSet<>();
        expected.add(new Tuple("a", 1.0));
        expected.add(new Tuple("b", 1.0));
        expected.add(new Tuple("c", 2.0));
        expected.add(new Tuple("d", 3.0));
        assertEquals(expected, results);
    }

    @TestTemplate
    public void testZUnionWithAggregateMax(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        jedis.zadd(ZSET_KEY_1, 3, "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        Set<Tuple> results = jedis.zunionWithScores(
                new ZParams().aggregate(ZParams.Aggregate.valueOf("MAX")), ZSET_KEY_1, ZSET_KEY_2);
        Set<Tuple> expected = new TreeSet<>();
        expected.add(new Tuple("a", 1.0));
        expected.add(new Tuple("b", 2.0));
        expected.add(new Tuple("c", 3.0));
        expected.add(new Tuple("d", 3.0));
        assertEquals(expected, results);
    }
}
