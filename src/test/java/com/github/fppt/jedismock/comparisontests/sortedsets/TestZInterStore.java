package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ZParams;
import redis.clients.jedis.resps.Tuple;

import java.util.List;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static redis.clients.jedis.params.ZParams.Aggregate.valueOf;

@ExtendWith(ComparisonBase.class)
public class TestZInterStore {

    private static final String ZSET_KEY_1 = "myzset";
    private static final String ZSET_KEY_2 = "ztmp";
    private static final String ZSET_KEY_OUT = "zout";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void testZInterStoreNotExistKeyToNotExistDest(Jedis jedis) {
        assertThat(jedis.zinterstore(ZSET_KEY_OUT, ZSET_KEY_1)).isEqualTo(0);
        assertThat(jedis.exists(ZSET_KEY_OUT)).isFalse();
    }

    @TestTemplate
    public void testZInterStoreWithEmptySet(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        assertThat(jedis.zinterstore(ZSET_KEY_OUT, ZSET_KEY_1, ZSET_KEY_2)).isEqualTo(0);
        assertThat(jedis.exists(ZSET_KEY_OUT)).isFalse();
    }

    @TestTemplate
    public void testZInterStoreBase(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        jedis.zadd(ZSET_KEY_1, 3, "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        assertThat(jedis.zinterstore(ZSET_KEY_OUT, ZSET_KEY_1, ZSET_KEY_2)).isEqualTo(2);
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertThat(results.get(0)).isEqualTo(new Tuple("b", 3.0));
        assertThat(results.get(1)).isEqualTo(new Tuple("c", 5.0));
    }

    @TestTemplate
    public void testZInterStoreWithWeights(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        jedis.zadd(ZSET_KEY_1, 3, "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        assertThat(jedis.zinterstore(ZSET_KEY_OUT, new ZParams().weights(2, 3), ZSET_KEY_1, ZSET_KEY_2)).isEqualTo(2);
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertThat(results.get(0)).isEqualTo(new Tuple("b", 7.0));
        assertThat(results.get(1)).isEqualTo(new Tuple("c", 12.0));
    }

    @TestTemplate
    public void testZInterStoreWithRegularSetAndWeights(Jedis jedis) {
        jedis.sadd(ZSET_KEY_1, "a", "b", "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        assertThat(jedis.zinterstore(ZSET_KEY_OUT, new ZParams().weights(2, 3), ZSET_KEY_1, ZSET_KEY_2)).isEqualTo(2);
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertThat(results.get(0)).isEqualTo(new Tuple("b", 5.0));
        assertThat(results.get(1)).isEqualTo(new Tuple("c", 8.0));
    }

    @TestTemplate
    public void testZInterStoreWithAggregateMin(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        jedis.zadd(ZSET_KEY_1, 3, "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        assertThat(jedis.zinterstore(ZSET_KEY_OUT,
                new ZParams().aggregate(valueOf("MIN")), ZSET_KEY_1, ZSET_KEY_2)).isEqualTo(2);
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertThat(results.get(0)).isEqualTo(new Tuple("b", 1.0));
        assertThat(results.get(1)).isEqualTo(new Tuple("c", 2.0));
    }

    @TestTemplate
    public void testZInterStoreWithAggregateMax(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        jedis.zadd(ZSET_KEY_1, 3, "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        assertThat(jedis.zinterstore(ZSET_KEY_OUT,
                new ZParams().aggregate(valueOf("MAX")), ZSET_KEY_1, ZSET_KEY_2)).isEqualTo(2);
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertThat(results.get(0)).isEqualTo(new Tuple("b", 2.0));
        assertThat(results.get(1)).isEqualTo(new Tuple("c", 3.0));
    }

    @TestTemplate
    public void testZInterStoreWithInfScores(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, Double.POSITIVE_INFINITY, "a");
        jedis.zadd(ZSET_KEY_2, Double.POSITIVE_INFINITY, "a");
        jedis.zinterstore(ZSET_KEY_OUT, ZSET_KEY_1, ZSET_KEY_2);
        assertThat(jedis.zscore(ZSET_KEY_OUT, "a")).isEqualTo(POSITIVE_INFINITY);

        jedis.zadd(ZSET_KEY_1, Double.NEGATIVE_INFINITY, "a");
        jedis.zadd(ZSET_KEY_2, Double.POSITIVE_INFINITY, "a");
        jedis.zinterstore(ZSET_KEY_OUT, ZSET_KEY_1, ZSET_KEY_2);
        assertThat(jedis.zscore(ZSET_KEY_OUT, "a")).isEqualTo(0);

        jedis.zadd(ZSET_KEY_1, Double.POSITIVE_INFINITY, "a");
        jedis.zadd(ZSET_KEY_2, Double.NEGATIVE_INFINITY, "a");
        jedis.zinterstore(ZSET_KEY_OUT, ZSET_KEY_1, ZSET_KEY_2);
        assertThat(jedis.zscore(ZSET_KEY_OUT, "a")).isEqualTo(0);

        jedis.zadd(ZSET_KEY_1, Double.NEGATIVE_INFINITY, "a");
        jedis.zadd(ZSET_KEY_2, Double.NEGATIVE_INFINITY, "a");
        jedis.zinterstore(ZSET_KEY_OUT, ZSET_KEY_1, ZSET_KEY_2);
        assertThat(jedis.zscore(ZSET_KEY_OUT, "a")).isEqualTo(NEGATIVE_INFINITY);
    }

    @TestTemplate
    public void testZInterStoreWithNanScores(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_2, 1, "a");
        assertThatThrownBy(() -> jedis.zinterstore(ZSET_KEY_OUT,
                new ZParams().weights(NaN, NaN), ZSET_KEY_1, ZSET_KEY_2))
                .isInstanceOf(RuntimeException.class);
    }
}
