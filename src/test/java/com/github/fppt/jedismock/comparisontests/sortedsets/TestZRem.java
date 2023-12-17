package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class TestZRem {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
    }

    @TestTemplate
    public void zRemRemovesKey(Jedis jedis) {
        Map<String, Double> members = new HashMap<>();
        members.put("myvalue1", 10d);
        members.put("myvalue2", 20d);

        long result = jedis.zadd(ZSET_KEY, members);
        assertThat(result).isEqualTo(2L);

        List<String> results = jedis.zrange(ZSET_KEY, 0, -1);
        assertThat(results).containsExactly("myvalue1", "myvalue2");

        result = jedis.zrem(ZSET_KEY, "myvalue1");
        assertThat(result).isEqualTo(1L);

        results = jedis.zrange(ZSET_KEY, 0, -1);
        assertThat(results).containsExactly("myvalue2");
    }

    @TestTemplate
    public void testZRemLastValue(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1, "aaa");
        assertThat(jedis.zrem(ZSET_KEY, "aaa")).isEqualTo(1);
        assertThat(jedis.exists(ZSET_KEY)).isFalse();
    }

    @TestTemplate
    public void testZRemKeyNotExist(Jedis jedis) {
        assertThat(jedis.zrem(ZSET_KEY, "aaa")).isEqualTo(0);
        assertThat(jedis.exists(ZSET_KEY)).isFalse();
    }
}
