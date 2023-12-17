package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class TestZCount {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void clearKey(Jedis jedis) {
        jedis.del(ZSET_KEY);
    }

    @TestTemplate
    public void whenUsingZCount_EnsureItReturnsZeroForNonDefinedKey(Jedis jedis) {
        assertThat(jedis.zcount(ZSET_KEY, "-inf", "+inf")).isEqualTo(0);
    }

    @TestTemplate
    public void whenUsingZCount_EnsureItReturnsEverythingWithPlusMinusInfinity(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 1, "two");
        jedis.zadd(ZSET_KEY, 1, "three");
        assertThat(jedis.zcount(ZSET_KEY, "-inf", "+inf")).isEqualTo(3);
    }

    @TestTemplate
    public void whenUsingzrangeByScore_EnsureItReturnsValueWhenIntervalSpecified(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        assertThat(jedis.zcard(ZSET_KEY)).isEqualTo(3);

        // then
        assertThat(jedis.zcount(ZSET_KEY, "-inf", "2")).isEqualTo(2);

    }

    @TestTemplate
    public void whenUsingzrangeByScore_EnsureItDoesNotReturnValueWhenExclusiveIntervalSpecified(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        assertThat(jedis.zcard(ZSET_KEY)).isEqualTo(3);

        // then
        assertThat(jedis.zcount(ZSET_KEY, "-inf", "(2")).isEqualTo(1);
        assertThat(jedis.zcount(ZSET_KEY, "(2", "+inf")).isEqualTo(1);
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
        assertThat(jedis.zcard(ZSET_KEY)).isEqualTo(10);

        //then
        assertThat(jedis.zcount(ZSET_KEY, 5, 8)).isEqualTo(4);
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
        assertThat(jedis.zcount(ZSET_KEY, -1, 1)).isEqualTo(3);
    }

    @TestTemplate
    public void whenUsingzrangeByScore_EnsureItThrowsExceptionsWhenStartAndEndHaveWrongFormat(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");

        // then
        assertThatThrownBy(() -> jedis.zcount(ZSET_KEY, "(dd", "(sd"))
                .isInstanceOf(JedisDataException.class);
        assertThatThrownBy(() -> jedis.zcount(ZSET_KEY, "1.e", "2.d"))
                .isInstanceOf(JedisDataException.class);
        assertThatThrownBy(() -> jedis.zcount(ZSET_KEY, "FOO", "BAR"))
                .isInstanceOf(RuntimeException.class);
    }

    @TestTemplate
    public void sameValuesDeduplication(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "one");
        jedis.zadd(ZSET_KEY, 3, "one");
        assertThat(jedis.zcount(ZSET_KEY, "-inf", "+inf")).isEqualTo(1);
        assertThat(jedis.zscore(ZSET_KEY, "one")).isEqualTo(3);
    }
}
