package com.github.fppt.jedismock.comparisontests.hashes;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class TestHDel {
    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
    }

    @TestTemplate
    public void whenHDeleting_EnsureValuesAreRemoved(Jedis jedis) {
        String field = "my-field-2";
        String hash = "my-hash-2";
        String value = "my-value-2";

        assertThat(jedis.hdel(hash, field)).isEqualTo(0);
        jedis.hset(hash, field, value);
        assertThat(jedis.hget(hash, field)).isEqualTo(value);
        assertThat(jedis.hdel(hash, field)).isEqualTo(1);
        assertThat(jedis.hget(hash, field)).isNull();
    }

    @TestTemplate
    void whenHDeletingWithManyArguments_EnsureValuesAreRemoved(Jedis jedis) {
        Map<String, String> hash = new HashMap<>();
        hash.put("key1", "1");
        hash.put("key2", "2");
        jedis.hset("foo", hash);
        final Long res = jedis.hdel("foo", "key1", "key2");
        assertThat(res).isEqualTo(2);
        assertThat(jedis.hgetAll("foo")).isEmpty();
    }

    @TestTemplate
    void whenHDeletingWithNoArguments_EnsureErrorReturn(Jedis jedis) {
        Map<String, String> hash = new HashMap<>();
        hash.put("key1", "1");
        hash.put("key2", "2");
        jedis.hset("foo", hash);
        assertThatThrownBy(() -> jedis.hdel("foo"))
                .isInstanceOf(RuntimeException.class);
        assertThat(jedis.hlen("foo")).isEqualTo(2);
    }
}
