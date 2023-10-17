package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.resps.Tuple;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(ComparisonBase.class)
public class TestZincrBy {

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void incrToExistsValue(Jedis jedis) {
        String key = "mykey";
        double old_score = 10d;
        double incr_score = -5d;
        String value = "myvalue";

        jedis.zadd(key, old_score, value);

        double result = jedis.zincrby(key, incr_score, value);
        assertEquals(old_score + incr_score, result);

        List<Tuple> results = jedis.zrangeWithScores(key, 0, -1);

        assertEquals(1, results.size());
        assertEquals(value, results.get(0).getElement());
        assertEquals(old_score + incr_score, results.get(0).getScore());
    }

    @TestTemplate
    public void incrToEmptyKey(Jedis jedis) {
        String key = "mykey";
        double incr_score = 10d;
        String value = "myvalue";

        double result = jedis.zincrby(key, incr_score, value);
        assertEquals(incr_score, result);

        List<Tuple> results = jedis.zrangeWithScores(key, 0, -1);

        assertEquals(1, results.size());
        assertEquals(value, results.get(0).getElement());
        assertEquals(incr_score, results.get(0).getScore());
    }
}
