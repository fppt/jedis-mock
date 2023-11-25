package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@ExtendWith(ComparisonBase.class)
public class TestZDiffStore {

    private static final String ZSET_KEY_1 = "myzset";
    private static final String ZSET_KEY_2 = "ztmp";
    private static final String ZSET_KEY_3 = "znew";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
        jedis.zadd(ZSET_KEY_1, 0, "a");
        jedis.zadd(ZSET_KEY_1, 1, "b");
        jedis.zadd(ZSET_KEY_1, 2, "c");
        jedis.zadd(ZSET_KEY_1, 3, "d");
        jedis.zadd(ZSET_KEY_2, 0, "a");
        jedis.zadd(ZSET_KEY_2, 1, "b");
    }

    @TestTemplate
    public void testZDiffStoreFromSmallSortedSet(Jedis jedis) {
        assertEquals(2, jedis.zdiffStore(ZSET_KEY_3, ZSET_KEY_1, ZSET_KEY_2));
        assertEquals(Arrays.asList("c", "d"), jedis.zrange(ZSET_KEY_3, 0, -1));
    }

    @TestTemplate
    public void testZDiffStoreFromBigSortedSet(Jedis jedis) {
        assertEquals(0, jedis.zdiffStore(ZSET_KEY_3, ZSET_KEY_2, ZSET_KEY_1));
        assertEquals(Collections.EMPTY_LIST, jedis.zrange(ZSET_KEY_3, 0, -1));
    }

    @TestTemplate
    public void testZDiffStoreFromItSelf(Jedis jedis) {
        assertEquals(0, jedis.zdiffStore(ZSET_KEY_3, ZSET_KEY_1, ZSET_KEY_1));
        assertFalse(jedis.exists(ZSET_KEY_3));
        assertEquals(Collections.EMPTY_LIST, jedis.zrange(ZSET_KEY_3, 0, -1));
    }

    @TestTemplate
    public void testZDiffStoreFromAnotherSet(Jedis jedis) {
        jedis.zadd(ZSET_KEY_3, 10, "bbb");
        assertEquals(4, jedis.zdiffStore(ZSET_KEY_2, ZSET_KEY_1, ZSET_KEY_3));
        assertEquals(Arrays.asList("a", "b", "c", "d"), jedis.zrange(ZSET_KEY_2, 0, -1));
    }

    @TestTemplate
    public void testZDiffStoreFromMultiplySets(Jedis jedis) {
        jedis.zadd("aaa", 10, "a");
        jedis.zadd("bbb", 10, "b");
        jedis.zadd("ddd", 10, "d");

        assertEquals(1, jedis.zdiffStore(ZSET_KEY_3, ZSET_KEY_1, "aaa", "bbb", "ddd"));
        assertEquals(Collections.singletonList("c"), jedis.zrange(ZSET_KEY_3, 0, -1));
    }
}
