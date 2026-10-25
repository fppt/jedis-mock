package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.KeyValue;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class TestZRank {

    private static final String ZSET_KEY = "myzset";

    @TestTemplate
    public void testZRankGetFirstRank(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 2, "bbb");
        jedis.zadd(ZSET_KEY, 3, "ccc");
        jedis.zadd(ZSET_KEY, 1, "aaa");
        jedis.zadd(ZSET_KEY, 5, "yyy");
        jedis.zadd(ZSET_KEY, 4, "xxx");

        assertThat(jedis.zrank(ZSET_KEY, "aaa")).isEqualTo(0);
    }

    @TestTemplate
    public void testZRankGetAllRanks(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 10, "x");
        jedis.zadd(ZSET_KEY, 20, "y");
        jedis.zadd(ZSET_KEY, 30, "z");

        assertThat(jedis.zrank(ZSET_KEY, "x")).isEqualTo(0);
        assertThat(jedis.zrank(ZSET_KEY, "y")).isEqualTo(1);
        assertThat(jedis.zrank(ZSET_KEY, "z")).isEqualTo(2);

        assertThat(jedis.zrank(ZSET_KEY, "foo")).isNull();
    }

    @TestTemplate
    public void testZRankWithScoreFractionalScore(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1.5, "a");
        jedis.zadd(ZSET_KEY, 2.25, "b");

        KeyValue<Long, Double> rank = jedis.zrankWithScore(ZSET_KEY, "b");
        assertThat(rank.getKey()).isEqualTo(1L);
        assertThat(rank.getValue()).isEqualTo(2.25);

        KeyValue<Long, Double> revRank = jedis.zrevrankWithScore(ZSET_KEY, "a");
        assertThat(revRank.getKey()).isEqualTo(1L);
        assertThat(revRank.getValue()).isEqualTo(1.5);
    }
}
