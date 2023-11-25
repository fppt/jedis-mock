package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.resps.Tuple;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(ComparisonBase.class)
public class TestZPopMax {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();

    }

    @TestTemplate
    public void testZPopMaxFromSingleKey(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 0, "a");
        jedis.zadd(ZSET_KEY, 1, "b");
        jedis.zadd(ZSET_KEY, 2, "c");

        assertEquals(new Tuple("c", 2.0), jedis.zpopmax(ZSET_KEY));
        assertEquals(new Tuple("b", 1.0), jedis.zpopmax(ZSET_KEY));
        assertEquals(new Tuple("a", 0.0), jedis.zpopmax(ZSET_KEY));
    }

    @TestTemplate
    public void testZPopMaxWithCount(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 0, "a");
        jedis.zadd(ZSET_KEY, 1, "b");
        jedis.zadd(ZSET_KEY, 2, "c");

        List<Tuple> expected = new ArrayList<>();
        expected.add(new Tuple("c", 2.0));
        expected.add(new Tuple("b", 1.0));

        assertEquals(expected, jedis.zpopmax(ZSET_KEY, 2));
    }

    @TestTemplate
    public void testZPopMaxFromEmptyKey(Jedis jedis) {
        assertNull(jedis.zpopmax(ZSET_KEY));
    }

    @TestTemplate
    public void testZPopMinWithNegativeCount(Jedis jedis) {
        jedis.set(ZSET_KEY, "foo");
        assertThrows(RuntimeException.class,
                () -> jedis.zpopmax(ZSET_KEY, -1));

        jedis.del(ZSET_KEY);
        assertThrows(RuntimeException.class,
                () -> jedis.zpopmax(ZSET_KEY, -2));

        jedis.zadd(ZSET_KEY, 1, "a");
        jedis.zadd(ZSET_KEY, 2, "b");
        jedis.zadd(ZSET_KEY, 3, "c");
        assertThrows(RuntimeException.class,
                () -> jedis.zpopmax(ZSET_KEY, -3));

    }
}
