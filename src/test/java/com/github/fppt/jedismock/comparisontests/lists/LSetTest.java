package com.github.fppt.jedismock.comparisontests.lists;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class LSetTest {
    private static final String key = "lset_list_key";
    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
        jedis.rpush(key, "1", "2", "3");
    }

    @TestTemplate
    @DisplayName("Change element")
    public void whenUsingLSet_EnsureElementIsSet(Jedis jedis) {
        jedis.lset(key, 1, "5");
        assertThat(jedis.lindex(key, 1)).isEqualTo("5");
    }

    @TestTemplate
    @DisplayName("Change element by negative index")
    public void whenUsingLSet_EnsureNegativeIndexWorks(Jedis jedis) {
        jedis.lset(key, -2, "5");
        assertThat(jedis.lindex(key, 1)).isEqualTo("5");
    }

    @TestTemplate
    @DisplayName("Check index out of bound returns error")
    public void whenUsingLSet_EnsureErrorOnIndexOutOfBounds(Jedis jedis) {
        assertThatThrownBy(() -> jedis.lset(key, 10, "5"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR index out of range");
        assertThatThrownBy(() -> jedis.lset(key, -10, "5"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR index out of range");
    }

    @TestTemplate
    @DisplayName("Check error on non existing key")
    public void whenUsingLSet_EnsureErrorOnNonExistingKey(Jedis jedis) {
        jedis.del(key);
        assertThatThrownBy(() -> jedis.lset(key, 0, "1"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR no such key");
    }
}
