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
        long count = jedis.del(key1, key2);
        assertEquals(2, count);
        assertFalse(jedis.exists(key1));
        assertFalse(jedis.exists(key2));
    }

    @TestTemplate
    public void unlinkingRemovesKeys(Jedis jedis) {
        jedis.set("key1", "Hello");
        jedis.set("key2", "World");
        long count = jedis.unlink("key1", "key2", "key3");
        assertEquals(2, count);
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

    @TestTemplate
    public void handleCurlyBraces(Jedis jedis) {
        jedis.mset("{hashslot}:one", "1", "{hashslot}:two", "2", "three", "3");

        Set<String> results = jedis.keys("{hashslot}:*");
        assertEquals(2, results.size());
        assertTrue(results.contains("{hashslot}:one") && results.contains("{hashslot}:two"));
    }

    @TestTemplate
    public void setNotExistsAfterAllElementsRemoved(Jedis jedis) {
        jedis.sadd("foo", "bar");
        jedis.srem("foo", "bar");
        assertFalse(jedis.exists("foo"));
        assertEquals(-2, jedis.ttl("foo"));
    }

    @TestTemplate
    public void zSetNotExistsAfterAllElementsRemoved(Jedis jedis) {
        jedis.zadd("foo", 42, "bar");
        jedis.zrem("foo", "bar");
        assertFalse(jedis.exists("foo"));
        assertEquals(-2, jedis.ttl("foo"));
    }

    @TestTemplate
    public void zSetNotExistsAfterAllElementsRemovedByScore(Jedis jedis) {
        jedis.zadd("foo", 42, "bar");
        jedis.zremrangeByScore("foo", 41, 43);
        assertFalse(jedis.exists("foo"));
        assertEquals(-2, jedis.ttl("foo"));
    }

    @TestTemplate
    public void listNotExistsAfterAllElementsRemoved(Jedis jedis) {
        jedis.lpush("foo", "bar");
        jedis.lpop("foo");
        assertFalse(jedis.exists("foo"));
        assertEquals(-2, jedis.ttl("foo"));
    }

    @TestTemplate
    public void hsetNotExistsAfterAllElementsRemoved(Jedis jedis) {
        jedis.hset("foo", "bar", "baz");
        jedis.hdel("foo", "bar");
        assertFalse(jedis.exists("foo"));
        assertEquals(-2, jedis.ttl("foo"));
    }

    @TestTemplate
    public void multipleExpire (Jedis jedis){
        jedis.set("foo1", "bar");
        jedis.set("foo2", "bar");
        jedis.expire("foo1", 0);
        jedis.expire("foo2", 0);
        assertEquals(0, jedis.dbSize());
    }
}
