package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ZRangeParams;
import redis.clients.jedis.resps.Tuple;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(ComparisonBase.class)
public class TestZRangeWithByScoreArg {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void clearKey(Jedis jedis) {
        jedis.del(ZSET_KEY);
    }

    @TestTemplate
    public void whenUsingZrangeByScore_EnsureItReturnsEmptySetForNonDefinedKey(Jedis jedis) {
        assertEquals(emptyList(), jedis.zrange(ZSET_KEY, ZRangeParams.zrangeByScoreParams(Double.MIN_VALUE, Double.MAX_VALUE)));
        assertEquals(emptyList(), jedis.zrange(ZSET_KEY + " WITHSCORES", ZRangeParams.zrangeByScoreParams(Double.MIN_VALUE, Double.MAX_VALUE)));
    }

    @TestTemplate
    public void whenUsingZrangeByScore_EnsureItReturnsEverythingWithPlusMinusInfinity(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 1, "two");
        jedis.zadd(ZSET_KEY, 1, "three");
        assertTrue(asList("one", "two", "three").containsAll(
                jedis.zrange(ZSET_KEY, ZRangeParams.zrangeByScoreParams(Double.MIN_VALUE, Double.MAX_VALUE))));
        assertTrue(asList(new Tuple("one", 1.),
                new Tuple("two", 1.), new Tuple("three", 1.)).containsAll(
                jedis.zrangeWithScores(ZSET_KEY, ZRangeParams.zrangeByScoreParams(Double.MIN_VALUE, Double.MAX_VALUE))));
    }


    @TestTemplate
    public void whenUsingZrangeByScore_EnsureItReturnsSetWhenLowestAndHighestScoresSpecified(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        assertEquals(3, jedis.zrange(ZSET_KEY, 0, -1).size());

        // when
        final List<String> zrangeByScoreResult = jedis.zrange(ZSET_KEY, ZRangeParams.zrangeByScoreParams(Double.MIN_VALUE, Double.MAX_VALUE));

        // then
        assertEquals(asList("one", "two", "three"), zrangeByScoreResult);
        assertEquals(asList(new Tuple("one", 1.),
                new Tuple("two", 2.), new Tuple("three", 3.)),
                jedis.zrangeWithScores(ZSET_KEY, ZRangeParams.zrangeByScoreParams(Double.MIN_VALUE, Double.MAX_VALUE)));

    }

    @TestTemplate
    public void whenUsingzrangeByScore_EnsureItReturnsValueWhenIntervalSpecified(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        assertEquals(3, jedis.zrange(ZSET_KEY, 0, -1).size());

        // when
        final List<String> zrangeByScoreResult = jedis.zrange(ZSET_KEY, ZRangeParams.zrangeByScoreParams(Double.MIN_VALUE, 2));

        // then
        assertEquals(asList("one", "two"), zrangeByScoreResult);

    }

    @TestTemplate
    public void whenUsingzrangeByScore_EnsureItDoesNotReturnValueWhenExclusiveIntervalSpecified(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        assertEquals(3, jedis.zrange(ZSET_KEY, 0, -1).size());

        // then
        assertEquals(singletonList("one"), jedis.zrange(ZSET_KEY, ZRangeParams.zrangeByScoreParams(Double.MIN_VALUE, 1.99)));
        assertEquals(singletonList(new Tuple("one", 1.)),
                jedis.zrangeWithScores(ZSET_KEY, ZRangeParams.zrangeByScoreParams(Double.MIN_VALUE, 1.99)));
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
        assertEquals(10, jedis.zrange(ZSET_KEY, 0, -1).size());

        //then
        assertEquals(asList("five", "six", "seven", "eight"),
                jedis.zrangeByScore(ZSET_KEY, 5, 8));
        assertEquals(asList(
                new Tuple("five", 5.),
                new Tuple("six", 6.),
                new Tuple("seven", 7.),
                new Tuple("eight", 8.)),
                jedis.zrangeWithScores(ZSET_KEY, ZRangeParams.zrangeByScoreParams(5, 8)));
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
        assertEquals(asList("minusone", "zero", "one"),
                jedis.zrange(ZSET_KEY, ZRangeParams.zrangeByScoreParams(-1, 1)));
        assertEquals(asList(
                new Tuple("minusone", -1.),
                new Tuple("zero", 0.),
                new Tuple("one", 1.)),
                jedis.zrangeWithScores(ZSET_KEY, ZRangeParams.zrangeByScoreParams(-1, 1)));
    }
}
