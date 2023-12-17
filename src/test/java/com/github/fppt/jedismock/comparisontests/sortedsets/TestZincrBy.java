package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.resps.Tuple;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class TestZincrBy {

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void testIncrByToExistsValue(Jedis jedis) {
        String key = "mykey";
        double old_score = 10d;
        double increment = -5d;
        String value = "myvalue";

        jedis.zadd(key, old_score, value);

        double result = jedis.zincrby(key, increment, value);
        assertThat(result).isEqualTo(old_score + increment);

        List<Tuple> results = jedis.zrangeWithScores(key, 0, -1);

        assertThat(results).hasSize(1);
        assertThat(results.get(0).getElement()).isEqualTo(value);
        assertThat(results.get(0).getScore()).isEqualTo(old_score + increment);
    }

    @TestTemplate
    public void testIncrByToEmptyKey(Jedis jedis) {
        String key = "mykey";
        double increment = 10d;
        String value = "myvalue";

        double result = jedis.zincrby(key, increment, value);
        assertThat(result).isEqualTo(increment);

        List<Tuple> results = jedis.zrangeWithScores(key, 0, -1);

        assertThat(results).hasSize(1);
        assertThat(results.get(0).getElement()).isEqualTo(value);
        assertThat(results.get(0).getScore()).isEqualTo(increment);
    }

    @TestTemplate
    public void testIncrByInfinityIncrementAndScore(Jedis jedis) {
        String key = "mykey";
        Double plusInfIncrement = Double.POSITIVE_INFINITY;
        Double minusInfIncrement = Double.NEGATIVE_INFINITY;
        double plusInfScore = Double.POSITIVE_INFINITY;
        double minusInfScore = Double.NEGATIVE_INFINITY;
        String value1 = "value1";
        String value2 = "value2";
        String value3 = "value3";
        String value4 = "value4";

        jedis.zadd(key, minusInfScore, value1);
        assertThatThrownBy(() -> jedis.zincrby(key, plusInfIncrement, value1))
                .isInstanceOf(RuntimeException.class);
        assertThat(jedis.zincrby(key, minusInfIncrement, value1)).isEqualTo(minusInfIncrement);

        jedis.zadd(key, plusInfScore, value2);
        assertThatThrownBy(() -> jedis.zincrby(key, minusInfIncrement, value2))
                .isInstanceOf(RuntimeException.class);
        assertThat(jedis.zincrby(key, plusInfIncrement, value2)).isEqualTo(plusInfIncrement);

        jedis.zadd(key, minusInfScore, value3);
        assertThat(jedis.zincrby(key, 10d, value3)).isEqualTo(minusInfIncrement);

        jedis.zadd(key, plusInfScore, value4);
        assertThat(jedis.zincrby(key, 10d, value4)).isEqualTo(plusInfIncrement);
    }
}
