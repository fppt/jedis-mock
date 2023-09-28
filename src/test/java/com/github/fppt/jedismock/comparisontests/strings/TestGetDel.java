package com.github.fppt.jedismock.comparisontests.strings;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class TestGetDel {

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void testGetAndDel(Jedis jedis) {
        String key = "key";
        String value = "value";
        jedis.set(key, value);
        String deletedValue = jedis.getDel(key);
        assertEquals(value, deletedValue);
        assertFalse(jedis.exists(key));
    }

    @TestTemplate
    public void testGetAndDelNonExistKey(Jedis jedis) {
        String deletedValue = jedis.getDel("key");
        assertNull(deletedValue);
    }
}
