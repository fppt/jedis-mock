package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ZRangeParams;
import redis.clients.jedis.resps.Tuple;

import java.util.List;

import static java.lang.Double.MAX_VALUE;
import static java.lang.Double.MIN_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static redis.clients.jedis.params.ZRangeParams.zrangeByScoreParams;

@ExtendWith(ComparisonBase.class)
public class TestZRangeWithByScoreArg {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void clearKey(Jedis jedis) {
        jedis.del(ZSET_KEY);
    }

    @TestTemplate
    public void whenUsingZrangeByScore_EnsureItReturnsEmptySetForNonDefinedKey(Jedis jedis) {
        assertThat(jedis.zrange(ZSET_KEY, zrangeByScoreParams(MIN_VALUE, MAX_VALUE))).isEmpty();
        assertThat(jedis.zrange(ZSET_KEY + " WITHSCORES", zrangeByScoreParams(MIN_VALUE, MAX_VALUE))).isEmpty();
    }

    @TestTemplate
    public void whenUsingZrangeByScore_EnsureItReturnsEverythingWithPlusMinusInfinity(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 1, "two");
        jedis.zadd(ZSET_KEY, 1, "three");
        assertThat(jedis.zrange(ZSET_KEY, ZRangeParams.zrangeByScoreParams(Double.MIN_VALUE, Double.MAX_VALUE)))
                .contains("one", "two", "three");
        assertThat(jedis.zrangeWithScores(ZSET_KEY, ZRangeParams.zrangeByScoreParams(Double.MIN_VALUE, Double.MAX_VALUE)))
                .contains(new Tuple("one", 1.), new Tuple("two", 1.), new Tuple("three", 1.));
    }

    @TestTemplate
    public void whenUsingZrangeByScore_EnsureItReturnsSetWhenLowestAndHighestScoresSpecified(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        assertThat(jedis.zrange(ZSET_KEY, 0, -1)).hasSize(3);

        // when
        final List<String> zrangeByScoreResult = jedis.zrange(ZSET_KEY, ZRangeParams.zrangeByScoreParams(Double.MIN_VALUE, Double.MAX_VALUE));

        // then
        assertThat(zrangeByScoreResult).containsExactly("one", "two", "three");
        assertThat(jedis.zrangeWithScores(ZSET_KEY, zrangeByScoreParams(MIN_VALUE, MAX_VALUE))).containsExactly(new Tuple("one", 1.),
                new Tuple("two", 2.), new Tuple("three", 3.));

    }

    @TestTemplate
    public void whenUsingzrangeByScore_EnsureItReturnsValueWhenIntervalSpecified(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        assertThat(jedis.zrange(ZSET_KEY, 0, -1)).hasSize(3);

        // when
        final List<String> zrangeByScoreResult = jedis.zrange(ZSET_KEY, ZRangeParams.zrangeByScoreParams(Double.MIN_VALUE, 2));

        // then
        assertThat(zrangeByScoreResult).containsExactly("one", "two");

    }

    @TestTemplate
    public void whenUsingzrangeByScore_EnsureItDoesNotReturnValueWhenExclusiveIntervalSpecified(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        assertThat(jedis.zrange(ZSET_KEY, 0, -1)).hasSize(3);

        // then
        assertThat(jedis.zrange(ZSET_KEY, zrangeByScoreParams(MIN_VALUE, 1.99))).containsExactly("one");
        assertThat(jedis.zrangeWithScores(ZSET_KEY, zrangeByScoreParams(MIN_VALUE, 1.99))).containsExactly(new Tuple("one", 1.));
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
        assertThat(jedis.zrange(ZSET_KEY, 0, -1)).hasSize(10);

        //then
        assertThat(jedis.zrangeByScore(ZSET_KEY, 5, 8)).containsExactly("five", "six", "seven", "eight");
        assertThat(jedis.zrangeWithScores(ZSET_KEY, zrangeByScoreParams(5, 8))).containsExactly(new Tuple("five", 5.),
                new Tuple("six", 6.),
                new Tuple("seven", 7.),
                new Tuple("eight", 8.));
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
        assertThat(jedis.zrange(ZSET_KEY, zrangeByScoreParams(-1, 1))).containsExactly("minusone", "zero", "one");
        assertThat(jedis.zrangeWithScores(ZSET_KEY, zrangeByScoreParams(-1, 1))).containsExactly(new Tuple("minusone", -1.),
                new Tuple("zero", 0.),
                new Tuple("one", 1.));
    }
}
