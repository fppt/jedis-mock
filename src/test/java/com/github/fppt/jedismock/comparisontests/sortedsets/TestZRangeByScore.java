package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.resps.Tuple;

import java.util.List;

import static java.lang.Double.POSITIVE_INFINITY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class TestZRangeByScore {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void clearKey(Jedis jedis) {
        jedis.flushDB();
    }

    @TestTemplate
    public void whenUsingZrangeByScore_EnsureItReturnsEmptySetForNonDefinedKey(Jedis jedis) {
        assertThat(jedis.zrangeByScore(ZSET_KEY, "-inf", "+inf")).isEmpty();
        assertThat(jedis.zrangeByScoreWithScores(ZSET_KEY, "-inf", "+inf")).isEmpty();
    }

    @TestTemplate
    public void whenUsingZrangeByScore_EnsureItReturnsEverythingWithPlusMinusInfinity(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 1, "two");
        jedis.zadd(ZSET_KEY, 1, "three");
        assertThat(jedis.zrangeByScore(ZSET_KEY, "-inf", "+inf")).contains("one", "two", "three");
        assertThat(jedis.zrangeByScoreWithScores(ZSET_KEY, "-inf", "+inf"))
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
        final List<String> zrangeByScoreResult = jedis.zrangeByScore(ZSET_KEY, "-inf", "+inf");

        // then
        assertThat(zrangeByScoreResult).containsExactly("one", "two", "three");
        assertThat(jedis.zrangeByScoreWithScores(ZSET_KEY, "-inf", "+inf")).containsExactly(new Tuple("one", 1.),
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
        final List<String> zrangeByScoreResult = jedis.zrangeByScore(ZSET_KEY, "-inf", "2");

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
        assertThat(jedis.zrangeByScore(ZSET_KEY, "-inf", "(2")).containsExactly("one");
        assertThat(jedis.zrangeByScoreWithScores(ZSET_KEY, "-inf", "(2")).containsExactly(new Tuple("one", 1.));
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
        assertThat(jedis.zrangeByScoreWithScores(ZSET_KEY, 5, 8)).containsExactly(new Tuple("five", 5.),
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
        assertThat(jedis.zrangeByScore(ZSET_KEY, -1, 1)).containsExactly("minusone", "zero", "one");
        assertThat(jedis.zrangeByScoreWithScores(ZSET_KEY, -1, 1)).containsExactly(new Tuple("minusone", -1.),
                new Tuple("zero", 0.),
                new Tuple("one", 1.));
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
        assertThat(jedis.zrange(ZSET_KEY, 0, -1)).hasSize(10);

        //then
        assertThat(jedis.zrangeByScoreWithScores(ZSET_KEY, 2, 5, 1, 2)).containsExactly(new Tuple("three", 3.),
                new Tuple("four", 4.));
    }

    @TestTemplate
    public void whenUsingzrangeByScore_EnsureItThrowsExceptionsWhenStartAndEndHaveWrongFormat(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");

        // then
        assertThatThrownBy(() -> jedis.zrangeByScore(ZSET_KEY, "(dd", "(sd"))
                .isInstanceOf(JedisDataException.class);
        assertThatThrownBy(() -> jedis.zrangeByScoreWithScores(ZSET_KEY, "(dd", "(sd"))
                .isInstanceOf(JedisDataException.class);
        assertThatThrownBy(() -> jedis.zrangeByScore(ZSET_KEY, "1.e", "2.d"))
                .isInstanceOf(JedisDataException.class);
        assertThatThrownBy(() -> jedis.zrangeByScore(ZSET_KEY, "FOO", "BAR"))
                .isInstanceOf(RuntimeException.class);
    }

    @TestTemplate
    void sortElementsWithSameScoreLexicographically(Jedis jedis) {
        jedis.zadd("foo", 42, "def");
        jedis.zadd("foo", 42, "abc");
        final List<String> list = jedis.zrangeByScore("foo", 42, 42, 0, 1);
        assertThat(list).containsExactly("abc");
    }

    @TestTemplate
    void minusInfinity(Jedis jedis) {
        jedis.zadd("foo", 0, "abc");
        jedis.zadd("foo", 1, "def");
        final List<String> list = jedis.zrangeByScore("foo", "-inf", "+inf");
        assertThat(list).containsExactly("abc", "def");
    }

    @TestTemplate
    void outOfOrderBounds(Jedis jedis) {
        jedis.zadd("foo", 42, "bar");
        assertThat(jedis.zrangeByScore("foo", 5, 2)).isEmpty();
    }

    @TestTemplate
    void negativeCount(Jedis jedis) {
        jedis.zadd("foo", 17, "bar");
        assertThat(jedis.zrangeByScore("foo", 0, 42, 0, -1)).containsExactly("bar");
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
        assertThat(jedis.zrangeByScore(ZSET_KEY, "-inf", "2")).containsExactly("a", "b", "c");
        assertThat(jedis.zrangeByScore(ZSET_KEY, 0, 3)).containsExactly("b", "c", "d");
        assertThat(jedis.zrangeByScore(ZSET_KEY, 3, 6)).containsExactly("d", "e", "f");
        assertThat(jedis.zrangeByScore(ZSET_KEY, 4, POSITIVE_INFINITY)).containsExactly("e", "f", "g");
    }

    @TestTemplate
    void testZRangeByScoreInclusive(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1, "b");
        jedis.zadd(ZSET_KEY, 2, "c");
        jedis.zadd(ZSET_KEY, 3, "d");
        jedis.zadd(ZSET_KEY, 4, "e");
        jedis.zadd(ZSET_KEY, 5, "f");
        assertThat(jedis.zrangeByScore(ZSET_KEY, "4", "2")).isEmpty();
        assertThat(jedis.zrangeByScore(ZSET_KEY, "6", "+inf")).isEmpty();
        assertThat(jedis.zrangeByScore(ZSET_KEY, "-inf", "-6")).isEmpty();
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
        assertThat(jedis.zrangeByScore(ZSET_KEY, "(-inf", "(2")).containsExactly("b");
        assertThat(jedis.zrangeByScore(ZSET_KEY, "(0", "(3")).containsExactly("b", "c");
        assertThat(jedis.zrangeByScore(ZSET_KEY, "(3", "(6")).containsExactly("e", "f");
        assertThat(jedis.zrangeByScore(ZSET_KEY, "(4", "(+inf")).containsExactly("f");
    }

    @TestTemplate
    void testZRangeByScoreExclusive(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1, "b");
        jedis.zadd(ZSET_KEY, 2, "c");
        jedis.zadd(ZSET_KEY, 3, "d");
        jedis.zadd(ZSET_KEY, 4, "e");
        jedis.zadd(ZSET_KEY, 5, "f");
        assertThat(jedis.zrangeByScore(ZSET_KEY, "(4", "(2")).isEmpty();
        assertThat(jedis.zrangeByScore(ZSET_KEY, "2", "(2")).isEmpty();
        assertThat(jedis.zrangeByScore(ZSET_KEY, "(2", "2")).isEmpty();
        assertThat(jedis.zrangeByScore(ZSET_KEY, "(6", "(+inf")).isEmpty();
        assertThat(jedis.zrangeByScore(ZSET_KEY, "(-inf", "(-6")).isEmpty();
    }

    @TestTemplate
    void testZRangeByScoreEmptyInnerRange(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1, "b");
        jedis.zadd(ZSET_KEY, 2, "c");
        jedis.zadd(ZSET_KEY, 3, "d");
        jedis.zadd(ZSET_KEY, 4, "e");
        jedis.zadd(ZSET_KEY, 5, "f");
        assertThat(jedis.zrangeByScore(ZSET_KEY, "2.4", "2.6")).isEmpty();
        assertThat(jedis.zrangeByScore(ZSET_KEY, "(2.4", "2.6")).isEmpty();
        assertThat(jedis.zrangeByScore(ZSET_KEY, "2.4", "(2.6")).isEmpty();
        assertThat(jedis.zrangeByScore(ZSET_KEY, "(2.4", "(2.6")).isEmpty();
    }

    @TestTemplate
    void testZRangeByScoreNonValueMin(Jedis jedis) {
        assertThatThrownBy(() -> jedis.zrangeByScore("fooz", "str", "2.6"))
                .isInstanceOf(RuntimeException.class);
    }

    @TestTemplate
    void testZRangeNoLimitWithScore(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        jedis.zadd(ZSET_KEY, 4, "four");
        jedis.zadd(ZSET_KEY, 5, "five");
        assertThat(jedis.zrangeByScoreWithScores(ZSET_KEY, 2, 6, 3, -1)).containsExactly(new Tuple("five", 5.));
    }

}
