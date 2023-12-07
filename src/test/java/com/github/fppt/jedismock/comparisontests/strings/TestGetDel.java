package com.github.fppt.jedismock.comparisontests.strings;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class TestGetDel {

    String key = "key";
    String value = "value";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void testGetAndDel(Jedis jedis) {
        jedis.set(key, value);
        String deletedValue = jedis.getDel(key);
        assertThat(deletedValue).isEqualTo(value);
        assertThat(jedis.exists(key)).isFalse();
    }

    @TestTemplate
    public void testGetAndDelNonExistKey(Jedis jedis) {
        String deletedValue = jedis.getDel(key);
        assertThat(deletedValue).isNull();
        assertThat(jedis.exists(key)).isFalse();
    }

    @TestTemplate
    public void getAndDelNonStringKey(Jedis jedis) {
        jedis.hset(key, "foo", "bar");
        assertThatThrownBy(() -> jedis.getDel(key))
                .isInstanceOf(JedisDataException.class)
                .hasMessageStartingWith("WRONGTYPE");
        assertThat(jedis.hget(key, "foo")).isEqualTo("bar");
    }
}
