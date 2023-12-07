package com.github.fppt.jedismock.comparisontests.strings;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Integer.parseInt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class StringOperationsTest {

    private final static byte[] msg = new byte[]{(byte) 0xbe};

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void whenSettingKeyAndRetrievingIt_CorrectResultIsReturned(Jedis jedis) {
        String key = "key";
        String value = "value";

        assertThat(jedis.get(key)).isNull();
        jedis.set(key, value);
        assertThat(jedis.get(key)).isEqualTo(value);
    }

    @TestTemplate
    public void whenConcurrentlyIncrementingAndDecrementingCount_EnsureFinalCountIsCorrect(
            final Jedis jedis, HostAndPort hostAndPort) throws InterruptedException {
        String key = "my-count-tracker";
        int[] count = new int[]{1, 5, 6, 2, -9, -2, 10, 11, 5, -2, -2};

        jedis.set(key, "0");
        assertThat(parseInt(jedis.get(key))).isEqualTo(0);

        //Increase counts concurrently

        List<Callable<Void>> callables = new ArrayList<>();
        for (int i : count) {
            callables.add(() -> {
                try (Jedis client = new Jedis(hostAndPort.getHost(), hostAndPort.getPort())) {
                    client.incrBy(key, i);
                }
                return null;
            });
        }
        ExecutorService pool = Executors.newCachedThreadPool();
        pool.invokeAll(callables);
        pool.shutdownNow();
        //Check final count
        assertThat(parseInt(jedis.get(key))).isEqualTo(25);
    }

    @TestTemplate
    void concurrentIncrementOfOriginallyEmptyKey(final Jedis jedis, HostAndPort hostAndPort) throws InterruptedException {
        List<Callable<Void>> callables = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            callables.add(() -> {
                try (Jedis client = new Jedis(hostAndPort.getHost(), hostAndPort.getPort())) {
                    client.incr("testKey");
                }
                return null;
            });
        }
        ExecutorService pool = Executors.newCachedThreadPool();
        pool.invokeAll(callables);
        pool.shutdownNow();
        assertThat(parseInt(jedis.get("testKey"))).isEqualTo(5);
    }

    @TestTemplate
    public void incrDoesNotClearTtl(Jedis jedis) {
        String key = "mykey";
        jedis.set(key, "0");
        jedis.expire(key, 100L);

        jedis.incr(key);
        long ttl = jedis.ttl(key);

        assertThat(ttl).isGreaterThan(0);
    }

    @TestTemplate
    public void incrByDoesNotClearTtl(Jedis jedis) {
        String key = "mykey";
        jedis.set(key, "0");
        jedis.expire(key, 100L);

        jedis.incrBy(key, 10);
        long ttl = jedis.ttl(key);

        assertThat(ttl).isGreaterThan(0);
    }

    @TestTemplate
    public void whenIncrementingWithIncrByFloat_ensureValuesAreCorrect(Jedis jedis) {
        jedis.set("key", "0");
        jedis.incrByFloat("key", 1.);
        assertThat(jedis.get("key")).isEqualTo("1");
        jedis.incrByFloat("key", 1.5);
        assertThat(jedis.get("key")).isEqualTo("2.5");
    }

    @TestTemplate
    public void whenIncrementingWithIncrBy_ensureValuesAreCorrect(Jedis jedis) {
        jedis.set("key", "0");
        jedis.incrBy("key", 1);
        assertThat(jedis.get("key")).isEqualTo("1");
        jedis.incrBy("key", 2);
        assertThat(jedis.get("key")).isEqualTo("3");
    }

    @TestTemplate
    public void whenIncrementingText_ensureException(Jedis jedis) {
        jedis.set("key", "foo");
        assertThatThrownBy(() -> jedis.incrBy("key", 1))
                .isInstanceOf(JedisDataException.class);
        assertThatThrownBy(() -> jedis.incrByFloat("key", 1.5))
                .isInstanceOf(JedisDataException.class);
    }

    @TestTemplate
    public void decrDoesNotClearTtl(Jedis jedis) {
        String key = "mykey";
        jedis.set(key, "0");
        jedis.expire(key, 100L);

        jedis.decr(key);
        long ttl = jedis.ttl(key);

        assertThat(ttl).isGreaterThan(0);
    }

    @TestTemplate
    public void decrByDoesNotClearTtl(Jedis jedis) {
        String key = "mykey";
        jedis.set(key, "0");
        jedis.expire(key, 100L);

        jedis.decrBy(key, 10);
        long ttl = jedis.ttl(key);

        assertThat(ttl).isGreaterThan(0);
    }

    @TestTemplate
    public void testSetNXNonUTF8binary(Jedis jedis) {
        jedis.setnx("foo".getBytes(), msg);
        assertThat(jedis.get("foo".getBytes())).containsExactlyInAnyOrder(msg);
    }

    @TestTemplate
    public void testSetEXNonUTF8binary(Jedis jedis) {
        jedis.setex("foo".getBytes(), 100, msg);
        assertThat(jedis.get("foo".getBytes())).containsExactlyInAnyOrder(msg);
    }

    @TestTemplate
    public void testMsetNonUTF8binary(Jedis jedis) {
        jedis.mset("foo".getBytes(), msg);
        assertThat(jedis.get("foo".getBytes())).containsExactlyInAnyOrder(msg);
    }

    @TestTemplate
    public void testGetSetNonUTF8binary(Jedis jedis) {
        jedis.getSet("foo".getBytes(), msg);
        assertThat(jedis.get("foo".getBytes())).containsExactlyInAnyOrder(msg);
    }

    @TestTemplate
    public void testGetSetEmptyString(Jedis jedis) {
        jedis.getSet("foo", "");
        assertThat(jedis.get("foo")).isEqualTo("");
    }

}
