package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.resps.Tuple;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class TestZPopMax {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();

    }

    @TestTemplate
    public void testZPopMaxFromSingleKey(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 0, "a");
        jedis.zadd(ZSET_KEY, 1, "b");
        jedis.zadd(ZSET_KEY, 2, "c");

        assertThat(jedis.zpopmax(ZSET_KEY)).isEqualTo(new Tuple("c", 2.0));
        assertThat(jedis.zpopmax(ZSET_KEY)).isEqualTo(new Tuple("b", 1.0));
        assertThat(jedis.zpopmax(ZSET_KEY)).isEqualTo(new Tuple("a", 0.0));
    }

    @TestTemplate
    public void testZPopMaxWithCount(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 0, "a");
        jedis.zadd(ZSET_KEY, 1, "b");
        jedis.zadd(ZSET_KEY, 2, "c");

        List<Tuple> expected = new ArrayList<>();
        expected.add(new Tuple("c", 2.0));
        expected.add(new Tuple("b", 1.0));

        assertThat(jedis.zpopmax(ZSET_KEY, 2)).isEqualTo(expected);
    }

    @TestTemplate
    public void testZPopMaxFromEmptyKey(Jedis jedis) {
        assertThat(jedis.zpopmax(ZSET_KEY)).isNull();
    }

    @TestTemplate
    public void testZPopMinWithNegativeCount(Jedis jedis) {
        jedis.set(ZSET_KEY, "foo");
        assertThatThrownBy(() -> jedis.zpopmax(ZSET_KEY, -1))
                .isInstanceOf(RuntimeException.class);

        jedis.del(ZSET_KEY);
        assertThatThrownBy(() -> jedis.zpopmax(ZSET_KEY, -2))
                .isInstanceOf(RuntimeException.class);

        jedis.zadd(ZSET_KEY, 1, "a");
        jedis.zadd(ZSET_KEY, 2, "b");
        jedis.zadd(ZSET_KEY, 3, "c");
        assertThatThrownBy(() -> jedis.zpopmax(ZSET_KEY, -3))
                .isInstanceOf(RuntimeException.class);

    }
}
