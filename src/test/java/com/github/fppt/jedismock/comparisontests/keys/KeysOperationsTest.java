package com.github.fppt.jedismock.comparisontests.keys;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat(results).containsExactlyInAnyOrder("one", "two", "four");

        //Another simple regex
        results = jedis.keys("t??");
        assertThat(results).containsExactly("two");

        //All Keys
        results = jedis.keys("*");
        assertThat(results).containsExactlyInAnyOrder("one", "two", "three", "four");
    }

    @TestTemplate
    public void whenGettingKeys_EnsureExpiredKeysAreNotReturned(Jedis jedis) throws InterruptedException {
        jedis.hset("test", "key", "value");
        jedis.expire("test", 1L);
        assertThat(jedis.keys("*")).containsExactly("test");
        Thread.sleep(2000);
        assertThat(jedis.keys("*")).isEmpty();
    }

    @TestTemplate
    public void whenCreatingKeys_existsValuesUpdated(Jedis jedis) {
        jedis.set("foo", "bar");
        assertThat(jedis.exists("foo")).isTrue();

        assertThat(jedis.exists("non-existent")).isFalse();

        jedis.hset("bar", "baz", "value");
        assertThat(jedis.exists("bar")).isTrue();
    }

    @TestTemplate
    public void deletionRemovesKeys(Jedis jedis) {
        String key1 = "hey_toremove";
        String key2 = "hmap_toremove";
        jedis.set(key1, "value");
        jedis.hset(key2, "field", "value");
        assertThat(jedis.exists(key1)).isTrue();
        assertThat(jedis.exists(key2)).isTrue();
        long count = jedis.del(key1, key2);
        assertThat(count).isEqualTo(2);
        assertThat(jedis.exists(key1)).isFalse();
        assertThat(jedis.exists(key2)).isFalse();
    }

    @TestTemplate
    public void unlinkingRemovesKeys(Jedis jedis) {
        jedis.set("key1", "Hello");
        jedis.set("key2", "World");
        long count = jedis.unlink("key1", "key2", "key3");
        assertThat(count).isEqualTo(2);
    }

    @TestTemplate
    public void hashExpires(Jedis jedis) throws InterruptedException {
        String key = "mykey";
        String subkey = "mysubkey";

        jedis.hsetnx(key, subkey, "a");
        jedis.expire(key, 1L);

        Thread.sleep(2000);

        String result = jedis.hget(key, subkey);

        assertThat(result).isNull();
    }

    @TestTemplate
    public void testPersist(Jedis jedis) throws Exception {
        jedis.psetex("a", 300, "v");
        assertThat(jedis.ttl("a")).isLessThanOrEqualTo(300);
        jedis.persist("a");
        assertThat(jedis.ttl("a")).isEqualTo(-1);
        Thread.sleep(500);
        //Check that the value is still there
        assertThat(jedis.get("a")).isEqualTo("v");
    }

    @TestTemplate
    public void handleCurlyBraces(Jedis jedis) {
        jedis.mset("{hashslot}:one", "1", "{hashslot}:two", "2", "three", "3");

        Set<String> results = jedis.keys("{hashslot}:*");
        assertThat(results).containsExactlyInAnyOrder("{hashslot}:one", "{hashslot}:two");
    }

    @TestTemplate
    public void setNotExistsAfterAllElementsRemoved(Jedis jedis) {
        jedis.sadd("foo", "bar");
        jedis.srem("foo", "bar");
        assertThat(jedis.exists("foo")).isFalse();
        assertThat(jedis.ttl("foo")).isEqualTo(-2);
    }

    @TestTemplate
    public void zSetNotExistsAfterAllElementsRemoved(Jedis jedis) {
        jedis.zadd("foo", 42, "bar");
        jedis.zrem("foo", "bar");
        assertThat(jedis.exists("foo")).isFalse();
        assertThat(jedis.ttl("foo")).isEqualTo(-2);
    }

    @TestTemplate
    public void zSetNotExistsAfterAllElementsRemovedByScore(Jedis jedis) {
        jedis.zadd("foo", 42, "bar");
        jedis.zremrangeByScore("foo", 41, 43);
        assertThat(jedis.exists("foo")).isFalse();
        assertThat(jedis.ttl("foo")).isEqualTo(-2);
    }

    @TestTemplate
    public void listNotExistsAfterAllElementsRemoved(Jedis jedis) {
        jedis.lpush("foo", "bar");
        jedis.lpop("foo");
        assertThat(jedis.exists("foo")).isFalse();
        assertThat(jedis.ttl("foo")).isEqualTo(-2);
    }

    @TestTemplate
    public void hsetNotExistsAfterAllElementsRemoved(Jedis jedis) {
        jedis.hset("foo", "bar", "baz");
        jedis.hdel("foo", "bar");
        assertThat(jedis.exists("foo")).isFalse();
        assertThat(jedis.ttl("foo")).isEqualTo(-2);
    }

    @TestTemplate
    public void multipleExpire (Jedis jedis){
        jedis.set("foo1", "bar");
        jedis.set("foo2", "bar");
        jedis.expire("foo1", 0);
        jedis.expire("foo2", 0);
        assertThat(jedis.dbSize()).isEqualTo(0);
    }
}
