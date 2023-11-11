package com.github.fppt.jedismock.comparisontests.hashes;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

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

        assertEquals(0, jedis.hdel(hash, field));
        jedis.hset(hash, field, value);
        assertEquals(value, jedis.hget(hash, field));
        assertEquals(1, jedis.hdel(hash, field));
        assertNull(jedis.hget(hash, field));
    }

    @TestTemplate
    void whenHDeletingWithManyArguments_EnsureValuesAreRemoved(Jedis jedis) {
        Map<String, String> hash = new HashMap<>();
        hash.put("key1", "1");
        hash.put("key2", "2");
        jedis.hset("foo", hash);
        final Long res = jedis.hdel("foo", "key1", "key2");
        assertEquals(2, res);
        assertTrue(jedis.hgetAll("foo").isEmpty());
    }

    @TestTemplate
    void whenHDeletingWithNoArguments_EnsureErrorReturn(Jedis jedis) {
        Map<String, String> hash = new HashMap<>();
        hash.put("key1", "1");
        hash.put("key2", "2");
        jedis.hset("foo", hash);
        assertThrows(RuntimeException.class,
                () -> jedis.hdel("foo"));
        assertEquals(2, jedis.hlen("foo"));
    }
}
