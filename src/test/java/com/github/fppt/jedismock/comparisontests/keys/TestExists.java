package com.github.fppt.jedismock.comparisontests.keys;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class TestExists {
    @TestTemplate
    public void whenCreatingKeys_existsValuesUpdated(Jedis jedis) {
        jedis.set("foo", "bar");
        assertThat(jedis.exists("foo")).isTrue();

        assertThat(jedis.exists("non-existent")).isFalse();

        jedis.hset("bar", "baz", "value");
        assertThat(jedis.exists("bar")).isTrue();
    }

    @TestTemplate
    public void multiExists(Jedis jedis) {
        jedis.set("a", "1");
        jedis.set("b", "2");
        jedis.set("c", "3");
        assertThat(jedis.exists("a", "b", "c")).isEqualTo(3);
        assertThat(jedis.exists("a", "b", "d")).isEqualTo(2);
        assertThat(jedis.exists("d", "e", "f")).isEqualTo(0);
        assertThatThrownBy(() -> jedis.exists(new String[0])).hasMessageContaining(
                "wrong number of arguments for 'exists' command");
    }

    @TestTemplate
    public void emptyExists(Jedis jedis) {
        assertThatThrownBy(() -> jedis.exists(new String[0])).hasMessageContaining(
                "wrong number of arguments for 'exists' command");
    }

}
