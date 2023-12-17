package com.github.fppt.jedismock.comparisontests.strings;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class TestMGet {
    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void mget(Jedis jedis) {
        jedis.set("key1", "Hello");
        jedis.set("key2", "World");
        assertThat(jedis.mget("key1", "key2", "key3")).containsExactly("Hello", "World", null);
    }


    @TestTemplate
    public void mgetWithNonText(Jedis jedis) {
        jedis.set("key1", "Hello");
        jedis.hset("key2", "field", "World");
        assertThat(jedis.mget("key1", "key2")).containsExactly("Hello", null);
    }
}
