package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ZParams;
import redis.clients.jedis.resps.Tuple;

import java.util.Arrays;
import java.util.List;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static redis.clients.jedis.params.ZParams.Aggregate.valueOf;

@ExtendWith(ComparisonBase.class)
public class TestZUnionStore {

    private static final String ZSET_KEY_1 = "myzset";
    private static final String ZSET_KEY_2 = "ztmp";
    private static final String ZSET_KEY_OUT = "zout";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void testZUnionStoreNotExistKeyToNotExistDest(Jedis jedis) {
        assertThat(jedis.zunionstore(ZSET_KEY_OUT, ZSET_KEY_1)).isEqualTo(0);
        assertThat(jedis.exists(ZSET_KEY_OUT)).isFalse();
    }

    @TestTemplate
    public void testZUnionStoreWithEmptySet(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        assertThat(jedis.zunionstore(ZSET_KEY_OUT, ZSET_KEY_1, ZSET_KEY_2)).isEqualTo(2);
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertThat(results).contains(new Tuple("a", 1.0), new Tuple("b", 2.0));
    }

    @TestTemplate
    public void testZUnionStoreBaseUnion(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        jedis.zadd(ZSET_KEY_1, 3, "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        assertThat(jedis.zunionstore(ZSET_KEY_OUT, ZSET_KEY_1, ZSET_KEY_2)).isEqualTo(4);
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertThat(results).contains(new Tuple("a", 1.0), new Tuple("b", 3.0), new Tuple("d", 3.0), new Tuple("c", 5.0));
    }

    @TestTemplate
    public void testZUnionStoreWithWeights(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        jedis.zadd(ZSET_KEY_1, 3, "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        assertThat(jedis.zunionstore(ZSET_KEY_OUT, new ZParams().weights(2, 3), ZSET_KEY_1, ZSET_KEY_2)).isEqualTo(4);
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertThat(results).contains(new Tuple("a", 2.0), new Tuple("b", 7.0), new Tuple("d", 9.0), new Tuple("c", 12.0));
    }

    @TestTemplate
    public void testZUnionStoreWithRegularSetAndWeights(Jedis jedis) {
        jedis.sadd(ZSET_KEY_1, "a", "b", "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        assertThat(jedis.zunionstore(ZSET_KEY_OUT, new ZParams().weights(2, 3), ZSET_KEY_1, ZSET_KEY_2)).isEqualTo(4);
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertThat(results).contains(new Tuple("a", 2.0), new Tuple("b", 5.0), new Tuple("c", 8.0), new Tuple("d", 9.0));
    }

    @TestTemplate
    public void testZUnionStoreWithAggregateMin(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        jedis.zadd(ZSET_KEY_1, 3, "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        assertThat(jedis.zunionstore(ZSET_KEY_OUT,
                new ZParams().aggregate(valueOf("MIN")), ZSET_KEY_1, ZSET_KEY_2)).isEqualTo(4);
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertThat(results).contains(new Tuple("a", 1.0), new Tuple("b", 1.0), new Tuple("c", 2.0), new Tuple("d", 3.0));
    }

    @TestTemplate
    public void testZUnionStoreWithAggregateMax(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        jedis.zadd(ZSET_KEY_1, 3, "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        assertThat(jedis.zunionstore(ZSET_KEY_OUT,
                new ZParams().aggregate(valueOf("MAX")), ZSET_KEY_1, ZSET_KEY_2)).isEqualTo(4);
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertThat(results).contains(new Tuple("a", 1.0), new Tuple("b", 2.0), new Tuple("c", 3.0), new Tuple("d", 3.0));
    }

    @TestTemplate
    public void testZUnionStoreWithAggregateSum(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        jedis.zadd(ZSET_KEY_1, 3, "c");
        jedis.zadd(ZSET_KEY_2, 1, "b");
        jedis.zadd(ZSET_KEY_2, 2, "c");
        jedis.zadd(ZSET_KEY_2, 3, "d");
        assertThat(jedis.zunionstore(ZSET_KEY_OUT,
                new ZParams().aggregate(valueOf("SUM")), ZSET_KEY_1, ZSET_KEY_2)).isEqualTo(4);
        List<Tuple> results = Arrays.asList(new Tuple("a", 1.0),
                new Tuple("b", 3.0),
                new Tuple("d", 3.0),
                new Tuple("c", 5.0));
        assertThat(jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1)).isEqualTo(results);
    }

    @TestTemplate
    public void testZUnionStoreWithInfScores(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, Double.POSITIVE_INFINITY, "a");
        jedis.zadd(ZSET_KEY_2, Double.POSITIVE_INFINITY, "a");
        jedis.zunionstore(ZSET_KEY_OUT, ZSET_KEY_1, ZSET_KEY_2);
        assertThat(jedis.zscore(ZSET_KEY_OUT, "a")).isEqualTo(POSITIVE_INFINITY);

        jedis.zadd(ZSET_KEY_1, Double.NEGATIVE_INFINITY, "a");
        jedis.zadd(ZSET_KEY_2, Double.POSITIVE_INFINITY, "a");
        jedis.zunionstore(ZSET_KEY_OUT, ZSET_KEY_1, ZSET_KEY_2);
        assertThat(jedis.zscore(ZSET_KEY_OUT, "a")).isEqualTo(0);

        jedis.zadd(ZSET_KEY_1, Double.POSITIVE_INFINITY, "a");
        jedis.zadd(ZSET_KEY_2, Double.NEGATIVE_INFINITY, "a");
        jedis.zunionstore(ZSET_KEY_OUT, ZSET_KEY_1, ZSET_KEY_2);
        assertThat(jedis.zscore(ZSET_KEY_OUT, "a")).isEqualTo(0);

        jedis.zadd(ZSET_KEY_1, Double.NEGATIVE_INFINITY, "a");
        jedis.zadd(ZSET_KEY_2, Double.NEGATIVE_INFINITY, "a");
        jedis.zunionstore(ZSET_KEY_OUT, ZSET_KEY_1, ZSET_KEY_2);
        assertThat(jedis.zscore(ZSET_KEY_OUT, "a")).isEqualTo(NEGATIVE_INFINITY);
    }

    @TestTemplate
    public void testZUnionStoreWithNanScores(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_2, 1, "a");
        assertThatThrownBy(() -> jedis.zunionstore(ZSET_KEY_OUT,
                new ZParams().weights(NaN, NaN), ZSET_KEY_1, ZSET_KEY_2))
                .isInstanceOf(RuntimeException.class);
    }

    @TestTemplate
    public void testZUnionStoreNotCreateNaNScores(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, Double.NEGATIVE_INFINITY, "a");

        jedis.zunionstore(ZSET_KEY_OUT, new ZParams().weights(0), ZSET_KEY_1);
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertThat(results.get(0)).isEqualTo(new Tuple("a", 0.0));

        jedis.zunionstore(ZSET_KEY_OUT, new ZParams().weights(1), ZSET_KEY_1);
        results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertThat(results.get(0)).isEqualTo(new Tuple("a", NEGATIVE_INFINITY));

        jedis.zunionstore(ZSET_KEY_OUT, new ZParams().weights(-2), ZSET_KEY_1);
        results = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        assertThat(results.get(0)).isEqualTo(new Tuple("a", POSITIVE_INFINITY));
    }
}
