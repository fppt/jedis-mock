package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(ComparisonBase.class)
public class TestZScore {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void testZScoreNotExistKey(Jedis jedis) {
        assertNull(jedis.zscore(ZSET_KEY, "a"));
    }

    @TestTemplate
    public void testZScoreNotExistMember(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 2, "b");
        assertNull(jedis.zscore(ZSET_KEY, "a"));
    }

    @TestTemplate
    public void testZScoreOK(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 2, "a");
        jedis.zadd(ZSET_KEY, 2.5, "b");
        assertEquals(2, jedis.zscore(ZSET_KEY, "a"));
        assertEquals(2.5, jedis.zscore(ZSET_KEY, "b"));
    }

    @TestTemplate
    public void testZScoreDoubleRange(Jedis jedis) {
        double value = 179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368.00000000000000000;
        jedis.zadd(ZSET_KEY, value, "a");
        assertEquals(1.7976931348623157e+308, jedis.zscore(ZSET_KEY, "a"));
    }
}
