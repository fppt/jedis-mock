package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class TestZDiffStore {

    private static final String ZSET_KEY_1 = "myzset";
    private static final String ZSET_KEY_2 = "ztmp";
    private static final String ZSET_KEY_3 = "znew";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
        jedis.zadd(ZSET_KEY_1, 0, "a");
        jedis.zadd(ZSET_KEY_1, 1, "b");
        jedis.zadd(ZSET_KEY_1, 2, "c");
        jedis.zadd(ZSET_KEY_1, 3, "d");
        jedis.zadd(ZSET_KEY_2, 0, "a");
        jedis.zadd(ZSET_KEY_2, 1, "b");
    }

    @TestTemplate
    public void testZDiffStoreFromSmallSortedSet(Jedis jedis) {
        assertThat(jedis.zdiffStore(ZSET_KEY_3, ZSET_KEY_1, ZSET_KEY_2)).isEqualTo(2);
        assertThat(jedis.zrange(ZSET_KEY_3, 0, -1)).containsExactly("c", "d");
    }

    @TestTemplate
    public void testZDiffStoreFromBigSortedSet(Jedis jedis) {
        assertThat(jedis.zdiffStore(ZSET_KEY_3, ZSET_KEY_2, ZSET_KEY_1)).isEqualTo(0);
        assertThat(jedis.zrange(ZSET_KEY_3, 0, -1)).isEmpty();
    }

    @TestTemplate
    public void testZDiffStoreFromItSelf(Jedis jedis) {
        assertThat(jedis.zdiffStore(ZSET_KEY_3, ZSET_KEY_1, ZSET_KEY_1)).isEqualTo(0);
        assertThat(jedis.exists(ZSET_KEY_3)).isFalse();
        assertThat(jedis.zrange(ZSET_KEY_3, 0, -1)).isEmpty();
    }

    @TestTemplate
    public void testZDiffStoreFromAnotherSet(Jedis jedis) {
        jedis.zadd(ZSET_KEY_3, 10, "bbb");
        assertThat(jedis.zdiffStore(ZSET_KEY_2, ZSET_KEY_1, ZSET_KEY_3)).isEqualTo(4);
        assertThat(jedis.zrange(ZSET_KEY_2, 0, -1)).containsExactly("a", "b", "c", "d");
    }

    @TestTemplate
    public void testZDiffStoreFromMultiplySets(Jedis jedis) {
        jedis.zadd("aaa", 10, "a");
        jedis.zadd("bbb", 10, "b");
        jedis.zadd("ddd", 10, "d");

        assertThat(jedis.zdiffStore(ZSET_KEY_3, ZSET_KEY_1, "aaa", "bbb", "ddd")).isEqualTo(1);
        assertThat(jedis.zrange(ZSET_KEY_3, 0, -1)).containsExactly("c");
    }
}
