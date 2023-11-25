package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.resps.Tuple;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(ComparisonBase.class)
public class TestZDiff {

    private static final String ZSET_KEY_1 = "myzset";
    private static final String ZSET_KEY_2 = "ztmp";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void testZDiffNotExistKeyToNotExistDest(Jedis jedis) {
        assertEquals(Collections.emptySet(), jedis.zdiff(ZSET_KEY_1));
    }

    @TestTemplate
    public void testZDiffWithEmptySet(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        Set<Tuple> results = jedis.zdiffWithScores(ZSET_KEY_1, ZSET_KEY_2);
        Set<Tuple> expected = new TreeSet<>();
        expected.add(new Tuple("a", 1.0));
        expected.add(new Tuple("b", 2.0));
        assertEquals(expected, results);
    }

    @TestTemplate
    public void testZDiffBase(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "1");
        jedis.zadd(ZSET_KEY_1, 2, "2");
        jedis.zadd(ZSET_KEY_1, 3, "3");
        jedis.zadd(ZSET_KEY_2, 1, "1");
        jedis.zadd(ZSET_KEY_2, 3, "3");
        jedis.zadd(ZSET_KEY_2, 4, "4");
        Set<Tuple> results = jedis.zdiffWithScores(ZSET_KEY_1, ZSET_KEY_2);
        Set<Tuple> expected = new TreeSet<>();
        expected.add(new Tuple("2", 2.0));
        assertEquals(expected, results);
    }
}
