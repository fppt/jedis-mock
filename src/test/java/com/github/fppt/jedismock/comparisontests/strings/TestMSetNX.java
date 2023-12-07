package com.github.fppt.jedismock.comparisontests.strings;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class TestMSetNX {
    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void msetnx(Jedis jedis) {
        assertThat(jedis.msetnx("key1", "Hello", "key2", "there")).isEqualTo(1);
        assertThat(jedis.msetnx("key2", "new", "key3", "world")).isEqualTo(0);
        assertThat(jedis.mget("key1", "key2", "key3")).containsExactly("Hello", "there", null);
    }
}
