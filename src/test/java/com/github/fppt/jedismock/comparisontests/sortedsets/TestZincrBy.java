package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.resps.Tuple;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        assertEquals(old_score + increment, result);

        List<Tuple> results = jedis.zrangeWithScores(key, 0, -1);

        assertEquals(1, results.size());
        assertEquals(value, results.get(0).getElement());
        assertEquals(old_score + increment, results.get(0).getScore());
    }

    @TestTemplate
    public void testIncrByToEmptyKey(Jedis jedis) {
        String key = "mykey";
        double increment = 10d;
        String value = "myvalue";

        double result = jedis.zincrby(key, increment, value);
        assertEquals(increment, result);

        List<Tuple> results = jedis.zrangeWithScores(key, 0, -1);

        assertEquals(1, results.size());
        assertEquals(value, results.get(0).getElement());
        assertEquals(increment, results.get(0).getScore());
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
        assertThrows(RuntimeException.class,
                () -> jedis.zincrby(key, plusInfIncrement, value1));
        assertEquals(minusInfIncrement, jedis.zincrby(key, minusInfIncrement, value1));

        jedis.zadd(key, plusInfScore, value2);
        assertThrows(RuntimeException.class,
                () -> jedis.zincrby(key, minusInfIncrement, value2));
        assertEquals(plusInfIncrement, jedis.zincrby(key, plusInfIncrement, value2));

        jedis.zadd(key, minusInfScore, value3);
        assertEquals(minusInfIncrement, jedis.zincrby(key, 10d, value3));

        jedis.zadd(key, plusInfScore, value4);
        assertEquals(plusInfIncrement, jedis.zincrby(key, 10d, value4));
    }
}
