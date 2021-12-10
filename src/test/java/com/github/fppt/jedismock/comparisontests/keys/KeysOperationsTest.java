package com.github.fppt.jedismock.comparisontests.keys;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class KeysOperationsTest {

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void whenGettingKeys_EnsureCorrectKeysAreReturned(Jedis jedis) {
        jedis.mset("one", "1", "two", "2", "three", "3", "four", "4");

        //Check simple pattern
        Set<String> results = jedis.keys("*o*");
        assertEquals(3, results.size());
        assertTrue(results.contains("one") && results.contains("two") && results.contains("four"));

        //Another simple regex
        results = jedis.keys("t??");
        assertEquals(1, results.size());
        assertTrue(results.contains("two"));

        //All Keys
        results = jedis.keys("*");
        assertEquals(4, results.size());
        assertTrue(results.contains("one") && results.contains("two") && results.contains("three") && results.contains(
                "four"));
    }

    @TestTemplate
    public void whenGettingKeys_EnsureExpiredKeysAreNotReturned(Jedis jedis) throws InterruptedException {
        jedis.hset("test", "key", "value");
        jedis.expire("test", 1L);
        assertEquals(Collections.singleton("test"), jedis.keys("*"));
        Thread.sleep(2000);
        assertEquals(Collections.emptySet(), jedis.keys("*"));
    }

    @TestTemplate
    public void whenCreatingKeys_existsValuesUpdated(Jedis jedis) {
        jedis.set("foo", "bar");
        assertTrue(jedis.exists("foo"));

        assertFalse(jedis.exists("non-existent"));

        jedis.hset("bar", "baz", "value");
        assertTrue(jedis.exists("bar"));
    }

    @TestTemplate
    public void deletionRemovesKeys(Jedis jedis) {
        String key1 = "hey_toremove";
        String key2 = "hmap_toremove";
        jedis.set(key1, "value");
        jedis.hset(key2, "field", "value");
        assertTrue(jedis.exists(key1));
        assertTrue(jedis.exists(key2));
        int count = jedis.del(key1, key2).intValue();
        assertEquals(2, count);
        assertFalse(jedis.exists(key1));
        assertFalse(jedis.exists(key2));
    }

    @TestTemplate
    public void hashExpires(Jedis jedis) throws InterruptedException {
        String key = "mykey";
        String subkey = "mysubkey";

        jedis.hsetnx(key, subkey, "a");
        jedis.expire(key, 1L);

        Thread.sleep(2000);

        String result = jedis.hget(key, subkey);

        assertNull(result);
    }

    @TestTemplate
    public void testPersist(Jedis jedis) throws Exception {
        jedis.psetex("a", 300, "v");
        assertTrue(jedis.ttl("a") <= 300);
        jedis.persist("a");
        assertEquals(-1, jedis.ttl("a"));
        Thread.sleep(500);
        //Check that the value is still there
        assertEquals("v", jedis.get("a"));
    }
}
