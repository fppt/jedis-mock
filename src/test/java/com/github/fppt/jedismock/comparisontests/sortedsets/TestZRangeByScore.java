package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.ZRangeParams;
import redis.clients.jedis.resps.Tuple;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(ComparisonBase.class)
public class TestZRangeByScore {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void clearKey(Jedis jedis) {
        jedis.flushDB();
    }

    @TestTemplate
    public void whenUsingZrangeByScore_EnsureItReturnsEmptySetForNonDefinedKey(Jedis jedis) {
        assertEquals(emptyList(), jedis.zrangeByScore(ZSET_KEY, "-inf", "+inf"));
        assertEquals(emptyList(), jedis.zrangeByScoreWithScores(ZSET_KEY, "-inf", "+inf"));
    }

    @TestTemplate
    public void whenUsingZrangeByScore_EnsureItReturnsEverythingWithPlusMinusInfinity(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 1, "two");
        jedis.zadd(ZSET_KEY, 1, "three");
        assertTrue(asList("one", "two", "three").containsAll(
                jedis.zrangeByScore(ZSET_KEY, "-inf", "+inf")));
        assertTrue(asList(new Tuple("one", 1.),
                new Tuple("two", 1.), new Tuple("three", 1.)).containsAll(
                jedis.zrangeByScoreWithScores(ZSET_KEY, "-inf", "+inf")));
    }


    @TestTemplate
    public void whenUsingZrangeByScore_EnsureItReturnsSetWhenLowestAndHighestScoresSpecified(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        assertEquals(3, jedis.zrange(ZSET_KEY, 0, -1).size());

        // when
        final List<String> zrangeByScoreResult = jedis.zrangeByScore(ZSET_KEY, "-inf", "+inf");

        // then
        assertEquals(asList("one", "two", "three"), zrangeByScoreResult);
        assertEquals(asList(new Tuple("one", 1.),
                        new Tuple("two", 2.), new Tuple("three", 3.)),
                jedis.zrangeByScoreWithScores(ZSET_KEY, "-inf", "+inf"));

    }

    @TestTemplate
    public void whenUsingzrangeByScore_EnsureItReturnsValueWhenIntervalSpecified(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        assertEquals(3, jedis.zrange(ZSET_KEY, 0, -1).size());

        // when
        final List<String> zrangeByScoreResult = jedis.zrangeByScore(ZSET_KEY, "-inf", "2");

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
        assertEquals(singletonList("one"), jedis.zrangeByScore(ZSET_KEY, "-inf", "(2"));
        assertEquals(singletonList(new Tuple("one", 1.)),
                jedis.zrangeByScoreWithScores(ZSET_KEY, "-inf", "(2"));
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
                jedis.zrangeByScoreWithScores(ZSET_KEY, 5, 8));
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
                jedis.zrangeByScore(ZSET_KEY, -1, 1));
        assertEquals(asList(
                        new Tuple("minusone", -1.),
                        new Tuple("zero", 0.),
                        new Tuple("one", 1.)),
                jedis.zrangeByScoreWithScores(ZSET_KEY, -1, 1));
    }

    @TestTemplate
    public void whenUsingzrangeByScore_EnsureItReturnsValuesWithLimitAndOffset(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 4, "four");
        jedis.zadd(ZSET_KEY, 3, "three");
        jedis.zadd(ZSET_KEY, 5, "five");
        jedis.zadd(ZSET_KEY, 7, "seven");
        jedis.zadd(ZSET_KEY, 6, "six");
        jedis.zadd(ZSET_KEY, 8, "eight");
        jedis.zadd(ZSET_KEY, 9, "nine");
        jedis.zadd(ZSET_KEY, 10, "ten");
        assertEquals(10, jedis.zrange(ZSET_KEY, 0, -1).size());

        //then
        assertEquals(asList(
                        new Tuple("three", 3.),
                        new Tuple("four", 4.)),
                jedis.zrangeByScoreWithScores(ZSET_KEY, 2, 5, 1, 2));
    }

    @TestTemplate
    public void whenUsingzrangeByScore_EnsureItThrowsExceptionsWhenStartAndEndHaveWrongFormat(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");

        // then
        assertThrows(JedisDataException.class,
                () -> jedis.zrangeByScore(ZSET_KEY, "(dd", "(sd"));
        assertThrows(JedisDataException.class,
                () -> jedis.zrangeByScoreWithScores(ZSET_KEY, "(dd", "(sd"));
        assertThrows(JedisDataException.class,
                () -> jedis.zrangeByScore(ZSET_KEY, "1.e", "2.d"));
        assertThrows(RuntimeException.class,
                () -> jedis.zrangeByScore(ZSET_KEY, "FOO", "BAR"));
    }

    @TestTemplate
    void sortElementsWithSameScoreLexicographically(Jedis jedis) {
        jedis.zadd("foo", 42, "def");
        jedis.zadd("foo", 42, "abc");
        final List<String> list = jedis.zrangeByScore("foo", 42, 42, 0, 1);
        assertEquals(singletonList("abc"), list);
    }

    @TestTemplate
    void minusInfinity(Jedis jedis) {
        jedis.zadd("foo", 0, "abc");
        jedis.zadd("foo", 1, "def");
        final List<String> list = jedis.zrangeByScore("foo", "-inf", "+inf");
        assertEquals(Arrays.asList("abc", "def"), list);
    }

    @TestTemplate
    void outOfOrderBounds(Jedis jedis) {
        jedis.zadd("foo", 42, "bar");
        assertEquals(0, jedis.zrangeByScore("foo", 5, 2).size());
    }

    @TestTemplate
    void negativeCount(Jedis jedis) {
        jedis.zadd("foo", 17, "bar");
        assertEquals(Collections.singletonList("bar"),
                jedis.zrangeByScore("foo", 0, 42, 0, -1));
    }

    @TestTemplate
    void testZRangeByScoreInclusiveRange(Jedis jedis) {
        jedis.zadd(ZSET_KEY, Double.NEGATIVE_INFINITY, "a");
        jedis.zadd(ZSET_KEY, 1, "b");
        jedis.zadd(ZSET_KEY, 2, "c");
        jedis.zadd(ZSET_KEY, 3, "d");
        jedis.zadd(ZSET_KEY, 4, "e");
        jedis.zadd(ZSET_KEY, 5, "f");
        jedis.zadd(ZSET_KEY, Double.POSITIVE_INFINITY, "g");
        assertEquals(Arrays.asList("a", "b", "c"), jedis.zrangeByScore(ZSET_KEY, "-inf", "2"));
        assertEquals(Arrays.asList("b", "c", "d"), jedis.zrangeByScore(ZSET_KEY, 0, 3));
        assertEquals(Arrays.asList("d", "e", "f"), jedis.zrangeByScore(ZSET_KEY, 3, 6));
        assertEquals(Arrays.asList("e", "f", "g"), jedis.zrangeByScore(ZSET_KEY, 4, Double.POSITIVE_INFINITY));
    }

    @TestTemplate
    void testZRangeByScoreInclusive(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1, "b");
        jedis.zadd(ZSET_KEY, 2, "c");
        jedis.zadd(ZSET_KEY, 3, "d");
        jedis.zadd(ZSET_KEY, 4, "e");
        jedis.zadd(ZSET_KEY, 5, "f");
        assertEquals(Collections.emptyList(), jedis.zrangeByScore(ZSET_KEY, "4", "2"));
        assertEquals(Collections.emptyList(), jedis.zrangeByScore(ZSET_KEY, "6", "+inf"));
        assertEquals(Collections.emptyList(), jedis.zrangeByScore(ZSET_KEY, "-inf", "-6"));
    }

    @TestTemplate
    void testZRangeByScoreExclusiveRange(Jedis jedis) {
        jedis.zadd(ZSET_KEY, Double.NEGATIVE_INFINITY, "a");
        jedis.zadd(ZSET_KEY, 1, "b");
        jedis.zadd(ZSET_KEY, 2, "c");
        jedis.zadd(ZSET_KEY, 3, "d");
        jedis.zadd(ZSET_KEY, 4, "e");
        jedis.zadd(ZSET_KEY, 5, "f");
        jedis.zadd(ZSET_KEY, Double.POSITIVE_INFINITY, "g");
        assertEquals(singletonList("b"), jedis.zrangeByScore(ZSET_KEY, "(-inf", "(2"));
        assertEquals(Arrays.asList("b", "c"), jedis.zrangeByScore(ZSET_KEY, "(0", "(3"));
        assertEquals(Arrays.asList("e", "f"), jedis.zrangeByScore(ZSET_KEY, "(3", "(6"));
        assertEquals(singletonList("f"), jedis.zrangeByScore(ZSET_KEY, "(4", "(+inf"));
    }

    @TestTemplate
    void testZRangeByScoreExclusive(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1, "b");
        jedis.zadd(ZSET_KEY, 2, "c");
        jedis.zadd(ZSET_KEY, 3, "d");
        jedis.zadd(ZSET_KEY, 4, "e");
        jedis.zadd(ZSET_KEY, 5, "f");
        assertEquals(Collections.emptyList(), jedis.zrangeByScore(ZSET_KEY, "(4", "(2"));
        assertEquals(Collections.emptyList(), jedis.zrangeByScore(ZSET_KEY, "2", "(2"));
        assertEquals(Collections.emptyList(), jedis.zrangeByScore(ZSET_KEY, "(2", "2"));
        assertEquals(Collections.emptyList(), jedis.zrangeByScore(ZSET_KEY, "(6", "(+inf"));
        assertEquals(Collections.emptyList(), jedis.zrangeByScore(ZSET_KEY, "(-inf", "(-6"));
    }

    @TestTemplate
    void testZRangeByScoreEmptyInnerRange(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1, "b");
        jedis.zadd(ZSET_KEY, 2, "c");
        jedis.zadd(ZSET_KEY, 3, "d");
        jedis.zadd(ZSET_KEY, 4, "e");
        jedis.zadd(ZSET_KEY, 5, "f");
        assertEquals(Collections.emptyList(), jedis.zrangeByScore(ZSET_KEY, "2.4", "2.6"));
        assertEquals(Collections.emptyList(), jedis.zrangeByScore(ZSET_KEY, "(2.4", "2.6"));
        assertEquals(Collections.emptyList(), jedis.zrangeByScore(ZSET_KEY, "2.4", "(2.6"));
        assertEquals(Collections.emptyList(), jedis.zrangeByScore(ZSET_KEY, "(2.4", "(2.6"));
    }

    @TestTemplate
    void testZRangeByScoreNonValueMin(Jedis jedis) {
        assertThrows(RuntimeException.class,
                () -> jedis.zrangeByScore("fooz", "str", "2.6"));
    }

    @TestTemplate
    void testZRangeNoLimitWithScore(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        jedis.zadd(ZSET_KEY, 4, "four");
        jedis.zadd(ZSET_KEY, 5, "five");
        assertEquals(Collections.singletonList(new Tuple("five", 5.)),
                jedis.zrangeByScoreWithScores(ZSET_KEY, 2, 6, 3, -1));
    }

}
