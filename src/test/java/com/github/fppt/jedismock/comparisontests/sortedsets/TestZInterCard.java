package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(ComparisonBase.class)
public class TestZInterCard {

    private static final String ZSET_KEY_1 = "myzset";
    private static final String ZSET_KEY_2 = "ztmp";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void testZInterCardNotExistKeyToNotExistDest(Jedis jedis) {
        assertEquals(0, jedis.zintercard(ZSET_KEY_1));
        assertEquals(0, jedis.zintercard(0, ZSET_KEY_1));
    }

    @TestTemplate
    public void testZInterCardWithEmptySet(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        assertEquals(0, jedis.zintercard(ZSET_KEY_1, ZSET_KEY_2));
        assertEquals(0, jedis.zintercard(0, ZSET_KEY_1, ZSET_KEY_2));
    }

    @TestTemplate
    public void testZInterCardBaseInter(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "1");
        jedis.zadd(ZSET_KEY_1, 2, "2");
        jedis.zadd(ZSET_KEY_1, 3, "3");
        jedis.zadd(ZSET_KEY_2, 1, "1");
        jedis.zadd(ZSET_KEY_2, 3, "3");
        jedis.zadd(ZSET_KEY_2, 4, "4");
        assertEquals(2, jedis.zintercard(ZSET_KEY_1, ZSET_KEY_2));
        assertEquals(2, jedis.zintercard(0, ZSET_KEY_1, ZSET_KEY_2));
    }

    @TestTemplate
    public void testZInterCardWithLimits(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "1");
        jedis.zadd(ZSET_KEY_1, 2, "2");
        jedis.zadd(ZSET_KEY_1, 3, "3");
        jedis.zadd(ZSET_KEY_2, 1, "1");
        jedis.zadd(ZSET_KEY_2, 3, "3");
        jedis.zadd(ZSET_KEY_2, 4, "4");
        assertEquals(2, jedis.zintercard(ZSET_KEY_1, ZSET_KEY_2));
        assertEquals(2, jedis.zintercard(0, ZSET_KEY_1, ZSET_KEY_2));
        assertEquals(1, jedis.zintercard(1, ZSET_KEY_1, ZSET_KEY_2));
        assertEquals(2, jedis.zintercard(10, ZSET_KEY_1, ZSET_KEY_2));
    }


    @TestTemplate
    public void testZInterCardWithNegativeLimit(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "1");
        jedis.zadd(ZSET_KEY_1, 2, "2");
        jedis.zadd(ZSET_KEY_2, 1, "1");
        jedis.zadd(ZSET_KEY_2, 4, "4");
        assertEquals(1, jedis.zintercard(ZSET_KEY_1, ZSET_KEY_2));
        assertThrows(RuntimeException.class,
                () -> jedis.zintercard(-10, ZSET_KEY_1, ZSET_KEY_2));
    }
}
