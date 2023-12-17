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
public class TestZRemRangeByRank {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void clearKey(Jedis jedis) {
        jedis.flushDB();
        Map<String, Double> members = new HashMap<>();
        members.put("a", 1d);
        members.put("b", 2d);
        members.put("c", 3d);
        members.put("d", 4d);
        members.put("e", 5d);
        long result = jedis.zadd(ZSET_KEY, members);
        assertThat(result).isEqualTo(5L);
    }

    @TestTemplate
    public void testZRemRangeByRankInnerRange(Jedis jedis) {
        assertThat(jedis.zremrangeByRank(ZSET_KEY, 1, 3)).isEqualTo(3);
        assertThat(jedis.zrange(ZSET_KEY, 0, -1)).containsExactly("a", "e");
    }

    @TestTemplate
    public void testZRemRangeByRankStartUnderflow(Jedis jedis) {
        assertThat(jedis.zremrangeByRank(ZSET_KEY, -10, 0)).isEqualTo(1);
        assertThat(jedis.zrange(ZSET_KEY, 0, -1)).containsExactly("b", "c", "d", "e");
    }

    @TestTemplate
    public void testZRemRangeByRankStartOverflow(Jedis jedis) {
        assertThat(jedis.zremrangeByRank(ZSET_KEY, 10, -1)).isEqualTo(0);
        assertThat(jedis.zrange(ZSET_KEY, 0, -1)).containsExactly("a", "b", "c", "d", "e");
    }

    @TestTemplate
    public void testZRemRangeByRankEndUnderflow(Jedis jedis) {
        assertThat(jedis.zremrangeByRank(ZSET_KEY, 0, -10)).isEqualTo(0);
        assertThat(jedis.zrange(ZSET_KEY, 0, -1)).containsExactly("a", "b", "c", "d", "e");
    }

    @TestTemplate
    public void testZRemRangeByRankEndOverflow(Jedis jedis) {
        assertThat(jedis.zremrangeByRank(ZSET_KEY, 0, 10)).isEqualTo(5);
        assertThat(jedis.zrange(ZSET_KEY, 0, -1)).isEmpty();
    }

    @TestTemplate
    public void testZRemRangeByRankDeleteWhenEmpty(Jedis jedis) {
        assertThat(jedis.zremrangeByRank(ZSET_KEY, 0, 4)).isEqualTo(5);
        assertThat(jedis.exists(ZSET_KEY)).isFalse();
    }
}
