package com.github.fppt.jedismock.comparisontests.strings;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import static org.junit.jupiter.api.Assertions.*;

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
        assertEquals(value, deletedValue);
        assertFalse(jedis.exists(key));
    }

    @TestTemplate
    public void testGetAndDelNonExistKey(Jedis jedis) {
        String deletedValue = jedis.getDel(key);
        assertNull(deletedValue);
        assertFalse(jedis.exists(key));
    }

    @TestTemplate
    public void getAndDelNonStringKey(Jedis jedis) {
        jedis.hset(key, "foo", "bar");
        String message = assertThrows(JedisDataException.class, () -> jedis.getDel(key)).getMessage();
        assertTrue(message.startsWith("WRONGTYPE"));
        assertEquals("bar", jedis.hget(key, "foo"));
    }
}
