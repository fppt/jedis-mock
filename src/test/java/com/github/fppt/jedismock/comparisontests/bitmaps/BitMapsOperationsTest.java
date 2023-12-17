package com.github.fppt.jedismock.comparisontests.bitmaps;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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
            assertThat(jedis.getbit("bm", i)).isEqualTo(bits.contains(i));
        }
    }

    @TestTemplate
    void testGetStringRepresentation(Jedis jedis) {
        jedis.set("bm2".getBytes(), jedis.get("bm".getBytes()));
        for (int i = 0; i <= Collections.max(bits); i++) {
            assertThat(jedis.getbit("bm2", i)).isEqualTo(bits.contains(i));
        }
    }

    @TestTemplate
    public void testGetOperationRepeatable(Jedis jedis) {
        byte[] buf = jedis.get("bm".getBytes());
        jedis.set("bm2".getBytes(), buf);
        byte[] buf2 = jedis.get("bm2".getBytes());
        assertThat(buf2).containsExactlyInAnyOrder(buf);
    }


    @TestTemplate
    void testValueAftersetbit(Jedis jedis) {
        jedis.setbit("foo", 0L, true);
        assertThat(jedis.getbit("foo", 0L)).isTrue();
        jedis.setbit("foo", 1L, true);
        assertThat(jedis.getbit("foo", 0L)).isTrue();
    }

    @TestTemplate
    public void testStringAndBitmapGet(Jedis jedis) {
        jedis.set("something", "foo");
        jedis.setbit("something", 41, true);
        jedis.set("something2".getBytes(), jedis.get("something".getBytes()));
        assertThat(jedis.getbit("something2", 1)).isTrue();
        assertThat(jedis.getbit("something2", 41)).isTrue();
    }
}
