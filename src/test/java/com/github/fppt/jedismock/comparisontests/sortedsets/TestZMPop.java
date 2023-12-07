package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.resps.Tuple;
import redis.clients.jedis.util.KeyValue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import static redis.clients.jedis.args.SortedSetOption.MAX;
import static redis.clients.jedis.args.SortedSetOption.MIN;

@ExtendWith(ComparisonBase.class)
public class TestZMPop {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();

    }

    @TestTemplate
    public void testZMPopKeyNotExist(Jedis jedis) {
        assertThat(jedis.zmpop(MIN, "aaa")).isNull();
    }

    @TestTemplate
    public void testZMPopFromOneKey(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        KeyValue<String, List<Tuple>> expected = new KeyValue<>(ZSET_KEY, Collections.singletonList(new Tuple("one", 1.0)));
        assertThat(jedis.zmpop(MIN, ZSET_KEY)).isEqualTo(expected);

        List<Tuple> tupleList = Arrays.asList(new Tuple("two", 2.), new Tuple("three", 3.));
        assertThat(jedis.zrangeWithScores(ZSET_KEY, 0, -1)).isEqualTo(tupleList);
    }

    @TestTemplate
    public void testZMPopCount(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        KeyValue<String, List<Tuple>> expected = new KeyValue<>(ZSET_KEY,
                Arrays.asList(new Tuple("three", 3.0),
                              new Tuple("two", 2.0)));
        assertThat(jedis.zmpop(MAX, 10, ZSET_KEY)).isEqualTo(expected);
    }

    @TestTemplate
    public void testZMPopKeyNegativeCount(Jedis jedis) {
        assertThatThrownBy(() ->
                jedis.zmpop(MIN, -1, "aaa"))
                .isInstanceOf(RuntimeException.class);
    }

}
