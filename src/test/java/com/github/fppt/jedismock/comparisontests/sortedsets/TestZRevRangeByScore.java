package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class TestZRevRangeByScore {
    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
    }

    @TestTemplate
    void zRevRangeByScoreReturnsValues(Jedis jedis) {
        assertThat(jedis.zrevrangeByScore(ZSET_KEY, 3, 1)).containsExactly("three", "two", "one");
    }

    @TestTemplate
    void sortElementsWithSameScoreLexicographically(Jedis jedis) {
        jedis.zadd("foo", 42, "abc");
        jedis.zadd("foo", 42, "def");
        final List<String> list = jedis.zrevrangeByScore("foo", 42, 42, 0, 1);
        assertThat(list).containsExactly("def");
    }

    @TestTemplate
    void minusInfinity(Jedis jedis) {
        jedis.zadd("foo", 0, "abc");
        jedis.zadd("foo", 1, "def");
        final List<String> list = jedis.zrevrangeByScore("foo", "+inf", "-inf");
        assertThat(list).containsExactly("def", "abc");
    }

    @TestTemplate
    void outOfOrderBounds(Jedis jedis) {
        jedis.zadd("foo", 42, "bar");
        assertThat(jedis.zrevrangeByScore("foo", 2, 5)).isEmpty();
    }

    @TestTemplate
    void testZRevRangeByScoreInclusiveRange(Jedis jedis) {
        jedis.flushDB();
        jedis.zadd(ZSET_KEY, Double.NEGATIVE_INFINITY, "a");
        jedis.zadd(ZSET_KEY, 1, "b");
        jedis.zadd(ZSET_KEY, 2, "c");
        jedis.zadd(ZSET_KEY, 3, "d");
        jedis.zadd(ZSET_KEY, 4, "e");
        jedis.zadd(ZSET_KEY, 5, "f");
        jedis.zadd(ZSET_KEY, Double.POSITIVE_INFINITY, "g");
        assertThat(jedis.zrevrangeByScore(ZSET_KEY, "2", "-inf")).containsExactly("c", "b", "a");
        assertThat(jedis.zrevrangeByScore(ZSET_KEY, "3", "0")).containsExactly("d", "c", "b");
        assertThat(jedis.zrevrangeByScore(ZSET_KEY, "6", "3")).containsExactly("f", "e", "d");
        assertThat(jedis.zrevrangeByScore(ZSET_KEY, "+inf", "4")).containsExactly("g", "f", "e");
    }

    @TestTemplate
    void testZRevRangeByScoreInclusive(Jedis jedis) {
        jedis.flushDB();
        jedis.zadd(ZSET_KEY, 1, "b");
        jedis.zadd(ZSET_KEY, 2, "c");
        jedis.zadd(ZSET_KEY, 3, "d");
        jedis.zadd(ZSET_KEY, 4, "e");
        jedis.zadd(ZSET_KEY, 5, "f");
        assertThat(jedis.zrevrangeByScore(ZSET_KEY, "+inf", "6")).isEmpty();
        assertThat(jedis.zrevrangeByScore(ZSET_KEY, "-6", "-inf")).isEmpty();
    }

    @TestTemplate
    void testZRevRangeByScoreExclusiveRange(Jedis jedis) {
        jedis.flushDB();
        jedis.zadd(ZSET_KEY, Double.NEGATIVE_INFINITY, "a");
        jedis.zadd(ZSET_KEY, 1, "b");
        jedis.zadd(ZSET_KEY, 2, "c");
        jedis.zadd(ZSET_KEY, 3, "d");
        jedis.zadd(ZSET_KEY, 4, "e");
        jedis.zadd(ZSET_KEY, 5, "f");
        jedis.zadd(ZSET_KEY, Double.POSITIVE_INFINITY, "g");
        assertThat(jedis.zrevrangeByScore(ZSET_KEY, "(2", "(-inf")).containsExactly("b");
        assertThat(jedis.zrevrangeByScore(ZSET_KEY, "(3", "(0")).containsExactly("c", "b");
        assertThat(jedis.zrevrangeByScore(ZSET_KEY, "(6", "(3")).containsExactly("f", "e");
        assertThat(jedis.zrevrangeByScore(ZSET_KEY, "(+inf", "(4")).containsExactly("f");
    }

    @TestTemplate
    void testZRevRangeByScoreExclusive(Jedis jedis) {
        jedis.flushDB();
        jedis.zadd(ZSET_KEY, 1, "b");
        jedis.zadd(ZSET_KEY, 2, "c");
        jedis.zadd(ZSET_KEY, 3, "d");
        jedis.zadd(ZSET_KEY, 4, "e");
        jedis.zadd(ZSET_KEY, 5, "f");
        assertThat(jedis.zrevrangeByScore(ZSET_KEY, "(+inf", "(6")).isEmpty();
        assertThat(jedis.zrevrangeByScore(ZSET_KEY, "(-6", "(-inf")).isEmpty();
    }

}
