package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class TestZRemRangeByScore {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void clearKey(Jedis jedis) {
        jedis.flushDB();
    }

    @TestTemplate
    public void whenUsingZremrangeByScore_EnsureItReturnsZeroForNonDefinedKey(Jedis jedis) {
        assertThat(jedis.zremrangeByScore(ZSET_KEY, "-inf", "+inf")).isEqualTo(0);
    }

    @TestTemplate
    public void whenUsingZremrangeByScore_EnsureItClearsEverythingWithPlusMinusInfinity(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 1, "two");
        jedis.zadd(ZSET_KEY, 1, "three");

        assertThat(jedis.zremrangeByScore(ZSET_KEY, "-inf", "+inf")).isEqualTo(3);
        assertThat(jedis.zrange(ZSET_KEY, 0, -1)).isEmpty();
    }


    @TestTemplate
    public void whenUsingZremrangeByScore_EnsureItReturnsSetSizeWhenLowestAndHighestScoresSpecified(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        assertThat(jedis.zrange(ZSET_KEY, 0, -1)).hasSize(3);

        // when
        final Long zremrangeByScoreResult = jedis.zremrangeByScore(ZSET_KEY, "-inf", "+inf");

        // then
        assertThat(zremrangeByScoreResult).isEqualTo(3);
        assertThat(jedis.zrange(ZSET_KEY, 0, -1)).isEmpty();
    }

    @TestTemplate
    public void whenUsingZremrangeByScore_EnsureItRemovesValueWhenIntervalSpecified(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        assertThat(jedis.zrange(ZSET_KEY, 0, -1)).hasSize(3);

        // when
        final Long zremrangeByScoreResult = jedis.zremrangeByScore(ZSET_KEY, "-inf", "2");

        // then
        assertThat(zremrangeByScoreResult).isEqualTo(2);
        List<String> zrangeResult = jedis.zrange(ZSET_KEY, 0, -1);
        assertThat(zrangeResult).containsExactly("three");
    }

    @TestTemplate
    public void whenUsingZremrangeByScore_EnsureItDoesNotRemoveValueWhenExclusiveIntervalSpecified(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        assertThat(jedis.zrange(ZSET_KEY, 0, -1)).hasSize(3);

        // when
        final Long zremrangeByScoreResult = jedis.zremrangeByScore(ZSET_KEY, "-inf", "(2");

        // then
        assertThat(zremrangeByScoreResult).isEqualTo(1);
        final List<String> zrangeResult = jedis.zrange(ZSET_KEY, 0, -1);
        assertThat(zrangeResult).containsExactly("two", "three");
    }

    @TestTemplate
    public void whenUsingZremrangeByScore_EnsureItRemovesValuesAccordingToSpecifiedInterval(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        jedis.zadd(ZSET_KEY, 4, "four");
        jedis.zadd(ZSET_KEY, 5, "five");
        jedis.zadd(ZSET_KEY, 6, "six");
        jedis.zadd(ZSET_KEY, 7, "seven");
        jedis.zadd(ZSET_KEY, 8, "eight");
        jedis.zadd(ZSET_KEY, 9, "nine");
        jedis.zadd(ZSET_KEY, 10, "ten");
        assertThat(jedis.zrange(ZSET_KEY, 0, -1)).hasSize(10);

        // when
        final Long zremrangeByScoreResult = jedis.zremrangeByScore(ZSET_KEY, 5, 8);

        // then
        assertThat(zremrangeByScoreResult).isEqualTo(4);
        final List<String> zrangeResult = jedis.zrange(ZSET_KEY, 0, -1);
        assertThat(zrangeResult).containsExactly("one", "two", "three", "four", "nine", "ten");
    }


    @TestTemplate
    public void whenUsingZremrangeByScore_EnsureItThrowsExceptionsWhenStartAndEndHaveWrongFormat(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");

        // then
        assertThatThrownBy(() -> jedis.zremrangeByScore(ZSET_KEY, "(dd", "(sd"))
                .isInstanceOf(JedisDataException.class);
        assertThatThrownBy(() -> jedis.zremrangeByScore(ZSET_KEY, "1.e", "2.d"))
                .isInstanceOf(JedisDataException.class);
        assertThatThrownBy(() -> jedis.zremrangeByScore(ZSET_KEY, "FOO", "BAR"))
                .isInstanceOf(RuntimeException.class);
    }

    @TestTemplate
    public void whenUsingZremrangeByScore_ZCardIsDiminishing(Jedis jedis) {
        jedis.zadd("foo", 1, "bar1");
        jedis.zadd("foo", 2, "bar2");
        jedis.zremrangeByScore("foo", 0, 1);
        assertThat(jedis.zrange("foo", 0, -1)).containsExactly("bar2");
        assertThat(jedis.zcard("foo")).isEqualTo(1);
    }
}
