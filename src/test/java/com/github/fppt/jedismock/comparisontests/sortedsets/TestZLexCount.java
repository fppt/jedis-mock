package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class TestZLexCount {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
        Map<String, Double> members = new HashMap<>();
        members.put("alpha", 0d);
        members.put("bar", 0d);
        members.put("cool", 0d);
        members.put("down", 0d);
        members.put("elephant", 0d);
        members.put("foo", 0d);
        members.put("great", 0d);
        members.put("hill", 0d);
        members.put("omega", 0d);
        long result = jedis.zadd(ZSET_KEY, members);
        assertThat(result).isEqualTo(9L);
    }

    @TestTemplate
    public void testZLexCountAllElements(Jedis jedis) {
        assertThat(jedis.zlexcount(ZSET_KEY, "-", "+")).isEqualTo(9);
    }

    @TestTemplate
    public void testZLexCountNoneElements(Jedis jedis) {
        assertThat(jedis.zlexcount(ZSET_KEY, "+", "-")).isEqualTo(0);
    }

    @TestTemplate
    public void testZLexCountStartPositiveInfinite(Jedis jedis) {
        assertThat(jedis.zlexcount(ZSET_KEY, "+", "[c")).isEqualTo(0);
    }

    @TestTemplate
    public void testZLexCountFinishNegativeInfinite(Jedis jedis) {
        assertThat(jedis.zlexcount(ZSET_KEY, "[c", "-")).isEqualTo(0);
    }

    @TestTemplate
    public void testZLexCountIncludeTwoBounds(Jedis jedis) {
        assertThat(jedis.zlexcount(ZSET_KEY, "[bar", "[foo")).isEqualTo(5);
    }

    @TestTemplate
    public void testZLexCountIncludeStartBound(Jedis jedis) {
        assertThat(jedis.zlexcount(ZSET_KEY, "[bar", "(foo")).isEqualTo(4);
    }

    @TestTemplate
    public void testZLexCountIncludeFinishBound(Jedis jedis) {
        assertThat(jedis.zlexcount(ZSET_KEY, "(bar", "[foo")).isEqualTo(4);
    }

    @TestTemplate
    public void testZLexCountNotIncludeBounds(Jedis jedis) {
        assertThat(jedis.zlexcount(ZSET_KEY, "(bar", "(foo")).isEqualTo(3);
    }

    @TestTemplate
    public void testZLexCountErrorMinValue(Jedis jedis) {
        assertThatThrownBy(() -> jedis.zlexcount(ZSET_KEY, "bar", "(foo"))
                .isInstanceOf(RuntimeException.class);
    }

    @TestTemplate
    public void testZLexCountErrorMaxValue(Jedis jedis) {
        assertThatThrownBy(() -> jedis.zlexcount(ZSET_KEY, "(bar", "foo"))
                .isInstanceOf(RuntimeException.class);
    }
}
