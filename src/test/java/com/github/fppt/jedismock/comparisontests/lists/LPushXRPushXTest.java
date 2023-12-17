package com.github.fppt.jedismock.comparisontests.lists;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class LPushXRPushXTest {
    private final static String lpushxKey = "lpushx_test_key";
    private final static String rpushxKey = "rpushx_test_key";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void whenUsingLPushX_EnsureReturnsZeroOnNonList(Jedis jedis) {
        assertThat(jedis.lpushx(lpushxKey, "foo")).isEqualTo(0);
    }

    @TestTemplate
    public void whenUsingLPushX_EnsurePushesCorrectly(Jedis jedis) {
        jedis.lpush(lpushxKey, "fooo");
        assertThat(jedis.lpushx(lpushxKey, "bar", "foo")).isEqualTo(3);
        assertThat(jedis.lrange(lpushxKey, 0, 1)).containsExactly("foo", "bar");
    }

    @TestTemplate
    public void whenUsingRPushX_EnsureReturnsZeroOnNonList(Jedis jedis) {
        assertThat(jedis.lpushx(rpushxKey, "foo")).isEqualTo(0);
    }

    @TestTemplate
    public void whenUsingRPushX_EnsurePushesCorrectly(Jedis jedis) {
        jedis.rpush(rpushxKey, "fooo");
        assertThat(jedis.rpushx(rpushxKey, "bar", "foo")).isEqualTo(3);
        assertThat(jedis.lrange(rpushxKey, 1, 2)).containsExactly("bar", "foo");
    }
}
