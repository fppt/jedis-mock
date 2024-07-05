package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ZRangeParams;
import redis.clients.jedis.resps.Tuple;

import java.util.List;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.MIN_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import static redis.clients.jedis.params.ZRangeParams.zrangeByLexParams;
import static redis.clients.jedis.params.ZRangeParams.zrangeParams;

@ExtendWith(ComparisonBase.class)
public class TestZRange {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
        jedis.zadd(ZSET_KEY, 2, "aaaa");
        jedis.zadd(ZSET_KEY, 3, "bbbb");
        jedis.zadd(ZSET_KEY, 1, "cccc");
        jedis.zadd(ZSET_KEY, 3, "bcbb");
        jedis.zadd(ZSET_KEY, 3, "babb");
        assertThat(jedis.zcount(ZSET_KEY, MIN_VALUE, MAX_VALUE)).isEqualTo(5L);
    }

    @TestTemplate
    public void whenUsingZrange_EnsureItReturnsEverythingInRightOrderWithPlusMinusMaxInteger(Jedis jedis) {
        assertThat(jedis.zrange(ZSET_KEY, MIN_VALUE, MAX_VALUE)).containsExactly("cccc", "aaaa", "babb", "bbbb", "bcbb");
    }

    @TestTemplate
    public void whenUsingZrange_EnsureItReturnsListInRightOrderWithPositiveRange(Jedis jedis) {
        assertThat(jedis.zrange(ZSET_KEY, 1, 3)).containsExactly("aaaa", "babb", "bbbb");
    }

    @TestTemplate
    public void whenUsingZrange_EnsureItReturnsListInRightOrderWithNegativeRange(Jedis jedis) {
        assertThat(jedis.zrange(ZSET_KEY, -3, -1)).containsExactly("babb", "bbbb", "bcbb");
    }

    @TestTemplate
    public void whenUsingZrange_EnsureItReturnsListInRightOrderWithNegativeStartAndPositiveEndRange(Jedis jedis) {
        assertThat(jedis.zrange(ZSET_KEY, -5, 2)).containsExactly("cccc", "aaaa", "babb");
    }

    @TestTemplate
    public void whenUsingZrange_EnsureItReturnsListInRightOrderWithPositiveStartAndNegativeEndRange(Jedis jedis) {
        assertThat(jedis.zrange(ZSET_KEY, 1, -1)).containsExactly("aaaa", "babb", "bbbb", "bcbb");
    }

    @TestTemplate
    public void whenUsingZrange_EnsureItReturnsEmptyListWhenOutOfRangeStartIndex(Jedis jedis) {
        assertThat(jedis.zrange(ZSET_KEY, 6, -1)).isEmpty();
    }

    @TestTemplate
    public void whenUsingZrange_EnsureItReturnsEmptyListWhenOutOfRangeEndIndex(Jedis jedis) {
        assertThat(jedis.zrange(ZSET_KEY, 1, -6)).isEmpty();
    }

    @TestTemplate
    public void whenUsingZrange_EnsureItReturnsListInLexicographicOrderForSameScore(Jedis jedis) {
        jedis.zadd("foo", 42, "def");
        jedis.zadd("foo", 42, "abc");
        assertThat(jedis.zrange("foo", 0, -1)).containsExactly("abc", "def");
        assertThat(jedis.zrange("foo", zrangeParams(0, -1).rev())).containsExactly("def", "abc");
    }

    @TestTemplate
    public void zRangeWorksSimilarToZRevRangeByScore(Jedis jedis) {
        jedis.zadd("foo", 1, "one");
        jedis.zadd("foo", 2, "two");
        jedis.zadd("foo", 3, "three");
        final List<String> list = jedis.zrange("foo", ZRangeParams.zrangeByScoreParams(3, 1).rev());
        assertThat(list).containsExactly("three", "two", "one");
    }

    @TestTemplate
    public void zRangeWorksWithDoubleInZAdd(Jedis jedis) {
        jedis.flushDB();
        jedis.zadd(ZSET_KEY, 3.14, "pi");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 1.23456789, "many");
        final List<Tuple> result = jedis.zrangeWithScores(ZSET_KEY, 0, -1);
        assertThat(result.get(0).getElement()).isEqualTo("many");
        assertThat(result.get(0).getScore()).isEqualTo(1.23456789);
        assertThat(result.get(1).getElement()).isEqualTo("two");
        assertThat(result.get(1).getScore()).isEqualTo(2.0);
        assertThat(result.get(2).getElement()).isEqualTo("pi");
        assertThat(result.get(2).getScore()).isEqualTo(3.14);
    }

    @TestTemplate
    public void whenUsingZrange_EnsureItReturnsErrorWhenByLexAndWithscores(Jedis jedis) {
        assertThatThrownBy(() -> jedis.zrangeWithScores(ZSET_KEY, zrangeByLexParams("1", "-6")))
                .isInstanceOf(RuntimeException.class);
    }

    @TestTemplate
    public void whenUsingZrange_EnsureItReturnsErrorWhenLimitNotByLexNotByScore(Jedis jedis) {
        assertThatThrownBy(() -> jedis.zrange(ZSET_KEY, new ZRangeParams(1, -6).limit(1, 1)))
                .isInstanceOf(RuntimeException.class);
    }
}
