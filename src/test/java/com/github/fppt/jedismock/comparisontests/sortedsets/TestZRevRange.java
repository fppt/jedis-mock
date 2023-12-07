package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.MIN_VALUE;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class TestZRevRange {

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
    public void whenUsingZrevrange_EnsureItReturnsEverythingInRightOrderWithPlusMinusMaxInteger(Jedis jedis) {
        assertThat(new ArrayList<>(jedis.zrevrange(ZSET_KEY, MIN_VALUE, MAX_VALUE))).containsExactly("bcbb", "bbbb", "babb", "aaaa", "cccc");
    }

    @TestTemplate
    public void whenUsingZrevrange_EnsureItReturnsListInRightOrderWithPositiveRange(Jedis jedis) {
        assertThat(new ArrayList<>(jedis.zrevrange(ZSET_KEY, 1, 3))).containsExactly("bbbb", "babb", "aaaa");
    }

    @TestTemplate
    public void whenUsingZrevrange_EnsureItReturnsListInRightOrderWithNegativeRange(Jedis jedis) {
        assertThat(new ArrayList<>(jedis.zrevrange(ZSET_KEY, -3, -1))).containsExactly("babb", "aaaa", "cccc");
    }

    @TestTemplate
    public void whenUsingZrevrange_EnsureItReturnsListInRightOrderWithNegativeStartAndPositiveEndRange(Jedis jedis) {
        assertThat(new ArrayList<>(jedis.zrevrange(ZSET_KEY, -5, 2))).containsExactly("bcbb", "bbbb", "babb");
    }

    @TestTemplate
    public void whenUsingZrevrange_EnsureItReturnsListInRightOrderWithPositiveStartAndNegativeEndRange(Jedis jedis) {
        assertThat(new ArrayList<>(jedis.zrevrange(ZSET_KEY, 1, -1))).containsExactly("bbbb", "babb", "aaaa", "cccc");
    }

    @TestTemplate
    public void whenUsingZrevrange_EnsureItReturnsEmptyListWhenGiveWrongStartOrStop(Jedis jedis) {
        assertThat(jedis.zrevrange(ZSET_KEY, -50, -100)).isEmpty();
    }
}
