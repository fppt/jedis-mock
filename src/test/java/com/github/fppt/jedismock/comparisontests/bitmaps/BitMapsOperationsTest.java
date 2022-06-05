package com.github.fppt.jedismock.comparisontests.bitmaps;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(ComparisonBase.class)
public class BitMapsOperationsTest {

    private final List<Integer> bits = Arrays.asList(2, 3, 5, 10, 11, 14);

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
        for (int i : bits) {
            jedis.setbit("bm", i, true);
        }
    }

    @TestTemplate
    void testSetBitByBitValue(Jedis jedis) {
        for (int i = 0; i <= Collections.max(bits); i++) {
            assertEquals(bits.contains(i), jedis.getbit("bm", i));
        }
    }

    @TestTemplate
    void testGetStringRepresentation(Jedis jedis) {
        jedis.set("bm2".getBytes(), jedis.get("bm".getBytes()));
        for (int i = 0; i <= Collections.max(bits); i++) {
            assertEquals(bits.contains(i), jedis.getbit("bm2", i));
        }
    }

    @TestTemplate
    void testValueAftersetbit(Jedis jedis) {
        jedis.setbit("foo", 0L, true);
        assertTrue(jedis.getbit("foo", 0L));
        jedis.setbit("foo", 1L, true);
        assertTrue(jedis.getbit("foo", 0L));
    }

    @TestTemplate
    public void testStringAndBitmapGet(Jedis jedis) {
        jedis.set("something", "foo");
        jedis.setbit("something", 41, true);
        jedis.set("something2".getBytes(), jedis.get("something".getBytes()));
        assertTrue(jedis.getbit("something2", 1));
        assertTrue(jedis.getbit("something2", 41));
    }
}
