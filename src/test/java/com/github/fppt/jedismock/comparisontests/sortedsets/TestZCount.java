package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(ComparisonBase.class)
public class TestZCount {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void clearKey(Jedis jedis) {
        jedis.del(ZSET_KEY);
    }

    @TestTemplate
    public void whenUsingZCount_EnsureItReturnsZeroForNonDefinedKey(Jedis jedis) {
        assertEquals(0, jedis.zcount(ZSET_KEY, "-inf", "+inf"));
    }

    @TestTemplate
    public void whenUsingZCount_EnsureItReturnsEverythingWithPlusMinusInfinity(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 1, "two");
        jedis.zadd(ZSET_KEY, 1, "three");
        assertEquals(3,
                jedis.zcount(ZSET_KEY, "-inf", "+inf"));
    }

    @TestTemplate
    public void whenUsingzrangeByScore_EnsureItReturnsValueWhenIntervalSpecified(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        assertEquals(3, jedis.zcard(ZSET_KEY));

        // then
        assertEquals(2, jedis.zcount(ZSET_KEY, "-inf", "2"));

    }

    @TestTemplate
    public void whenUsingzrangeByScore_EnsureItDoesNotReturnValueWhenExclusiveIntervalSpecified(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        assertEquals(3, jedis.zcard(ZSET_KEY));

        // then
        assertEquals(1, jedis.zcount(ZSET_KEY, "-inf", "(2"));
        assertEquals(1, jedis.zcount(ZSET_KEY, "(2", "+inf"));
    }

    @TestTemplate
    public void whenUsingzrangeByScore_EnsureItReturnsValuesAccordingToSpecifiedInterval(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        jedis.zadd(ZSET_KEY, 4, "four");
        jedis.zadd(ZSET_KEY, 5, "five");
        jedis.zadd(ZSET_KEY, 7, "seven");
        jedis.zadd(ZSET_KEY, 6, "six");
        jedis.zadd(ZSET_KEY, 8, "eight");
        jedis.zadd(ZSET_KEY, 9, "nine");
        jedis.zadd(ZSET_KEY, 10, "ten");
        assertEquals(10, jedis.zcard(ZSET_KEY));

        //then
        assertEquals(4,
                jedis.zcount(ZSET_KEY, 5, 8));
    }


    @TestTemplate
    public void whenUsingzrangeByScore_EnsureItReturnsValuesAccordingToSpecifiedIntervalWithNegative(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, -2, "minustwo");
        jedis.zadd(ZSET_KEY, -1, "minusone");
        jedis.zadd(ZSET_KEY, 0, "zero");
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");

        //then
        assertEquals(3, jedis.zcount(ZSET_KEY, -1, 1));
    }

    @TestTemplate
    public void whenUsingzrangeByScore_EnsureItThrowsExceptionsWhenStartAndEndHaveWrongFormat(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");

        // then
        assertThrows(JedisDataException.class,
                () -> jedis.zcount(ZSET_KEY, "(dd", "(sd"));
        assertThrows(JedisDataException.class,
                () -> jedis.zcount(ZSET_KEY, "1.e", "2.d"));
        assertThrows(RuntimeException.class,
                () -> jedis.zcount(ZSET_KEY, "FOO", "BAR"));
    }

    @TestTemplate
    public void sameValuesDeduplication(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "one");
        jedis.zadd(ZSET_KEY, 3, "one");
        assertEquals(1,
                jedis.zcount(ZSET_KEY, "-inf", "+inf"));
        assertEquals(3, jedis.zscore(ZSET_KEY, "one"));
    }
}
