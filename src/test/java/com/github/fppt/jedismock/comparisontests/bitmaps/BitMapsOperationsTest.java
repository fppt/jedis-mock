package com.github.fppt.jedismock.comparisontests.bitmaps;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

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
        jedis.setbit("foo", 0L, false);
        assertThat(jedis.getbit("foo", 0L)).isFalse();

    }

    @TestTemplate
    public void testStringAndBitmapGet(Jedis jedis) {
        jedis.set("something", "foo");
        jedis.setbit("something", 41, true);
        jedis.set("something2".getBytes(), jedis.get("something".getBytes()));
        assertThat(jedis.getbit("something2", 1)).isTrue();
        assertThat(jedis.getbit("something2", 41)).isTrue();
    }

    @TestTemplate
    public void bitsToString(Jedis jedis) {
        jedis.setbit("bitmapsarestrings", 2, true);
        jedis.setbit("bitmapsarestrings", 3, true);
        jedis.setbit("bitmapsarestrings", 5, true);
        jedis.setbit("bitmapsarestrings", 10, true);
        jedis.setbit("bitmapsarestrings", 11, true);
        jedis.setbit("bitmapsarestrings", 14, true);
        String result = jedis.get("bitmapsarestrings");
        assertThat(result).isEqualTo("42");
    }

    @TestTemplate
    public void stringToBits(Jedis jedis) {
        jedis.set("foo", "e");
        assertThat(jedis.getbit("foo", 0)).isFalse();
        assertThat(jedis.getbit("foo", 1)).isTrue();
    }

    @TestTemplate
    public void testLongSetBit(Jedis jedis) {
        int len = 256 * 8;
        jedis.del("mykey");
        byte[] expectedBytes = new byte[256];
        Random random = new Random(42);
        for (int i = 0; i < 2000; i++) {
            int bitPosition = random.nextInt(len);
            boolean bitValue = random.nextBoolean();
            int byteIndex = bitPosition / 8;
            int bitIndex = 7 - (bitPosition % 8);
            if (bitValue) {
                expectedBytes[byteIndex] |= (byte) (1 << bitIndex);
            } else {
                expectedBytes[byteIndex] &= (byte) ~(1 << bitIndex);
            }
            jedis.setbit("mykey", bitPosition, bitValue);
            byte[] redisBytes = jedis.get("mykey".getBytes());
            byte[] expectedTruncated = new byte[redisBytes.length];
            System.arraycopy(expectedBytes, 0, expectedTruncated, 0, redisBytes.length);
            assertThat(redisBytes).isEqualTo(expectedTruncated);
        }
    }

    @TestTemplate
    public void testNoOp(Jedis jedis) {
        jedis.setbit("noop", 100, false);
        assertThat(jedis.get("noop")).hasSize((100 + 7) / 8);
    }

    @TestTemplate
    public void loadZeroes(Jedis jedis) {
        byte[] zeroes = new byte[17];
        jedis.set("zeroes".getBytes(), zeroes);
        jedis.setbit("zeroes", 0L, true);
        assertThat(jedis.get("zeroes".getBytes())).hasSize(17);
    }
}
