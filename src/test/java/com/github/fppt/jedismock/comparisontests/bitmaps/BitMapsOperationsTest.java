package com.github.fppt.jedismock.comparisontests.bitmaps;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import com.github.fppt.jedismock.datastructures.Slice;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class BitMapsOperationsTest {

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    void testValueAftersetbit(Jedis jedis){
        jedis.setbit("foo", 0L, true);
        assertEquals(jedis.getbit("foo", 0L), true);
        jedis.setbit("foo", 1L, true);
        assertEquals(jedis.getbit("foo", 0L), true);
        String str = jedis.get("foo");
        jedis.set("hi", str);
        assertEquals(jedis.getbit("hi", 0L), true);
        assertEquals(jedis.getbit("hi", 1L), true);
    }
}
