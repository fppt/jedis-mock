package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class TestZRemRangeByLex {

    private final String key = "mykey";

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
        long result = jedis.zadd(key, members);
        assertThat(result).isEqualTo(9L);
    }

    @TestTemplate
    public void testZRemRangeByLexInclusiveRangeNegInf(Jedis jedis) {
        assertThat(jedis.zremrangeByLex(key, "-", "[cool")).isEqualTo(3);
        assertThat(jedis.zrange(key, 0, -1)).containsExactly("down", "elephant", "foo", "great", "hill", "omega");
    }

    @TestTemplate
    public void testZRemRangeByLexInclusiveRangeInclude(Jedis jedis) {
        assertThat(jedis.zremrangeByLex(key, "[bar", "[down")).isEqualTo(3);
        assertThat(jedis.zrange(key, 0, -1)).containsExactly("alpha", "elephant", "foo", "great", "hill", "omega");
    }

    @TestTemplate
    public void testZRemRangeByLexInclusiveRangePosInf(Jedis jedis) {
        assertThat(jedis.zremrangeByLex(key, "[g", "+")).isEqualTo(3);
        assertThat(jedis.zrange(key, 0, -1)).containsExactly("alpha", "bar", "cool", "down", "elephant", "foo");
    }

    @TestTemplate
    public void testZRemRangeByLexExclusiveRangeNegInf(Jedis jedis) {
        assertThat(jedis.zremrangeByLex(key, "-", "(cool")).isEqualTo(2);
        assertThat(jedis.zrange(key, 0, -1)).containsExactly("cool", "down", "elephant", "foo", "great", "hill", "omega");
    }

    @TestTemplate
    public void testZRemRangeByLexExclusiveRangeInclude(Jedis jedis) {
        assertThat(jedis.zremrangeByLex(key, "(bar", "(down")).isEqualTo(1);
        assertThat(jedis.zrange(key, 0, -1)).containsExactly("alpha", "bar", "down", "elephant", "foo", "great", "hill", "omega");
    }

    @TestTemplate
    public void testZRemRangeByLexExclusiveRangePosInf(Jedis jedis) {
        assertThat(jedis.zremrangeByLex(key, "(great", "+")).isEqualTo(2);
        assertThat(jedis.zrange(key, 0, -1)).containsExactly("alpha", "bar", "cool", "down", "elephant", "foo", "great");
    }

    @TestTemplate
    public void testZRemRangeByLexInclusiveAndExclusiveRangeNegInf(Jedis jedis) {
        assertThat(jedis.zremrangeByLex(key, "-", "[aaaa")).isEqualTo(0);
        assertThat(jedis.zrange(key, 0, -1)).containsExactly("alpha", "bar", "cool", "down", "elephant", "foo", "great", "hill", "omega");
    }

    @TestTemplate
    public void testZRemRangeByLexInclusiveAndExclusiveRangeInclude(Jedis jedis) {
        assertThat(jedis.zremrangeByLex(key, "(az", "(b")).isEqualTo(0);
        assertThat(jedis.zrange(key, 0, -1)).containsExactly("alpha", "bar", "cool", "down", "elephant", "foo", "great", "hill", "omega");
    }

    @TestTemplate
    public void testZRemRangeByLexInclusiveAndExclusiveRangePosInf(Jedis jedis) {
        assertThat(jedis.zremrangeByLex(key, "(z", "+")).isEqualTo(0);
        assertThat(jedis.zrange(key, 0, -1)).containsExactly("alpha", "bar", "cool", "down", "elephant", "foo", "great", "hill", "omega");
    }

    @TestTemplate
    public void testZRemRangeByLexEmpty(Jedis jedis) {
        assertThat(jedis.zremrangeByLex(key, "-", "+")).isEqualTo(9);
        assertThat(jedis.zcard(key)).isEqualTo(0);
        assertThat(jedis.exists(key)).isFalse();
    }
}
