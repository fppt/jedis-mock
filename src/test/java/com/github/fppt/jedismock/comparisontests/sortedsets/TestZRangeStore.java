package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ZRangeParams;
import redis.clients.jedis.resps.Tuple;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import static redis.clients.jedis.Protocol.Keyword.BYLEX;
import static redis.clients.jedis.Protocol.Keyword.BYSCORE;

@ExtendWith(ComparisonBase.class)
public class TestZRangeStore {

    private static final String ZSET_KEY = "myzset";
    private static final String ZSET_KEY_OUT = "out";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
        jedis.zadd(ZSET_KEY, 1, "a");
        jedis.zadd(ZSET_KEY, 2, "b");
        jedis.zadd(ZSET_KEY, 3, "c");
        jedis.zadd(ZSET_KEY, 4, "d");
    }

    @TestTemplate
    public void testZRangeStoreBaseOK(Jedis jedis) {
        assertThat(jedis.zrangestore(ZSET_KEY_OUT, ZSET_KEY, new ZRangeParams(0, -1))).isEqualTo(4);
        List<Tuple> result = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        List<Tuple> expected = Arrays.asList(
                new Tuple("a", 1.0),
                new Tuple("b", 2.0),
                new Tuple("c", 3.0),
                new Tuple("d", 4.0));
        assertThat(result).isEqualTo(expected);
    }

    @TestTemplate
    public void testZRangeStoreRange(Jedis jedis) {
        assertThat(jedis.zrangestore(ZSET_KEY_OUT, ZSET_KEY, new ZRangeParams(1, 2))).isEqualTo(2);
        List<Tuple> result = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        List<Tuple> expected = Arrays.asList(
                new Tuple("b", 2.0),
                new Tuple("c", 3.0));
        assertThat(result).isEqualTo(expected);
    }

    @TestTemplate
    public void testZRangeStoreByLex(Jedis jedis) {
        assertThat(jedis.zrangestore(ZSET_KEY_OUT, ZSET_KEY, new ZRangeParams(BYLEX, "[b", "[c"))).isEqualTo(2);
        List<Tuple> result = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        List<Tuple> expected = Arrays.asList(
                new Tuple("b", 2.0),
                new Tuple("c", 3.0));
        assertThat(result).isEqualTo(expected);
    }

    @TestTemplate
    public void testZRangeStoreByLexAndRev(Jedis jedis) {
        assertThat(jedis.zrangestore(ZSET_KEY_OUT, ZSET_KEY, new ZRangeParams(BYLEX, "[c", "[b").rev())).isEqualTo(2);
        List<Tuple> result = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        List<Tuple> expected = Arrays.asList(
                new Tuple("b", 2.0),
                new Tuple("c", 3.0));
        assertThat(result).isEqualTo(expected);
    }

    @TestTemplate
    public void testZRangeStoreByScore(Jedis jedis) {
        assertThat(jedis.zrangestore(ZSET_KEY_OUT, ZSET_KEY, new ZRangeParams(BYSCORE, "0", "2.5"))).isEqualTo(2);
        List<Tuple> result = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        List<Tuple> expected = Arrays.asList(
                new Tuple("a", 1.0),
                new Tuple("b", 2.0));
        assertThat(result).isEqualTo(expected);
    }

    @TestTemplate
    public void testZRangeStoreByScoreAndRev(Jedis jedis) {
        assertThat(jedis.zrangestore(ZSET_KEY_OUT, ZSET_KEY, new ZRangeParams(BYSCORE, "2.5", "0").rev())).isEqualTo(2);
        List<Tuple> result = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        List<Tuple> expected = Arrays.asList(
                new Tuple("a", 1.0),
                new Tuple("b", 2.0));
        assertThat(result).isEqualTo(expected);
    }

    @TestTemplate
    public void testZRangeStoreByScoreAndLimit(Jedis jedis) {
        assertThat(jedis.zrangestore(ZSET_KEY_OUT, ZSET_KEY, new ZRangeParams(BYSCORE, "0", "5").limit(2, -1))).isEqualTo(2);
        List<Tuple> result = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        List<Tuple> expected = Arrays.asList(
                new Tuple("c", 3.0),
                new Tuple("d", 4.0));
        assertThat(result).isEqualTo(expected);
    }

    @TestTemplate
    public void testZRangeStoreByScoreAndRevAndLimit(Jedis jedis) {
        assertThat(jedis.zrangestore(ZSET_KEY_OUT, ZSET_KEY, new ZRangeParams(BYSCORE, "5", "0").rev().limit(0, 2))).isEqualTo(2);
        List<Tuple> result = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        List<Tuple> expected = Arrays.asList(
                new Tuple("c", 3.0),
                new Tuple("d", 4.0));
        assertThat(result).isEqualTo(expected);
    }

    @TestTemplate
    public void testZRangeStoreEmptyRange(Jedis jedis) {
        assertThat(jedis.zrangestore(ZSET_KEY_OUT, ZSET_KEY, new ZRangeParams(5, 6))).isEqualTo(0);
        assertThat(jedis.exists(ZSET_KEY_OUT)).isFalse();

        assertThat(jedis.zrangestore(ZSET_KEY_OUT, ZSET_KEY, new ZRangeParams(BYSCORE, "5", "6"))).isEqualTo(0);
        assertThat(jedis.exists(ZSET_KEY_OUT)).isFalse();

        assertThat(jedis.zrangestore(ZSET_KEY_OUT, ZSET_KEY, new ZRangeParams(BYLEX, "[f", "[g"))).isEqualTo(0);
        assertThat(jedis.exists(ZSET_KEY_OUT)).isFalse();
    }

    @TestTemplate
    public void testZRangeStoreWrongTypeKey(Jedis jedis) {
        jedis.zadd(ZSET_KEY_OUT, 1, "a");
        jedis.set("foo", "bar");

        assertThatThrownBy(() -> jedis.zrangestore(ZSET_KEY_OUT, "foo", new ZRangeParams(0, -1)))
                .isInstanceOf(RuntimeException.class);
        assertThat(jedis.zrange(ZSET_KEY_OUT, 0, -1)).containsExactly("a");
    }

    @TestTemplate
    public void testZRangeStoreInvalidSyntax(Jedis jedis) {
        assertThatThrownBy(() -> jedis.zrangestore(ZSET_KEY_OUT, ZSET_KEY, new ZRangeParams(0, -1).limit(1, 2)))
                .isInstanceOf(RuntimeException.class);
    }

    @TestTemplate
    public void testZRangeStoreFromNoExistKey(Jedis jedis) {
        jedis.zadd(ZSET_KEY_OUT, 2, "aaa");
        assertThat(jedis.zrangestore(ZSET_KEY_OUT, "noKey", new ZRangeParams(0, -1))).isEqualTo(0);
        assertThat(jedis.exists(ZSET_KEY_OUT)).isFalse();
    }
}


