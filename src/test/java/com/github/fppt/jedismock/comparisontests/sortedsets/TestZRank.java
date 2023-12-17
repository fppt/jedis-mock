package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class TestZRank {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
    }

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
}
