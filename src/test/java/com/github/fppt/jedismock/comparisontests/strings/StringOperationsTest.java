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

import static org.junit.jupiter.api.Assertions.*;

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

        assertNull(jedis.get(key));
        jedis.set(key, value);
        assertEquals(value, jedis.get(key));
    }

    @TestTemplate
    public void whenConcurrentlyIncrementingAndDecrementingCount_EnsureFinalCountIsCorrect(
            final Jedis jedis, HostAndPort hostAndPort) throws InterruptedException {
        String key = "my-count-tracker";
        int[] count = new int[]{1, 5, 6, 2, -9, -2, 10, 11, 5, -2, -2};

        jedis.set(key, "0");
        assertEquals(0, Integer.parseInt(jedis.get(key)));

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
        assertEquals(25, Integer.parseInt(jedis.get(key)));
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
        assertEquals(5, Integer.parseInt(jedis.get("testKey")));
    }

    @TestTemplate
    public void incrDoesNotClearTtl(Jedis jedis) {
        String key = "mykey";
        jedis.set(key, "0");
        jedis.expire(key, 100L);

        jedis.incr(key);
        long ttl = jedis.ttl(key);

        assertTrue(ttl > 0);
    }

    @TestTemplate
    public void incrByDoesNotClearTtl(Jedis jedis) {
        String key = "mykey";
        jedis.set(key, "0");
        jedis.expire(key, 100L);

        jedis.incrBy(key, 10);
        long ttl = jedis.ttl(key);

        assertTrue(ttl > 0);
    }

    @TestTemplate
    public void whenIncrementingWithIncrByFloat_ensureValuesAreCorrect(Jedis jedis) {
        jedis.set("key", "0");
        jedis.incrByFloat("key", 1.);
        assertEquals("1", jedis.get("key"));
        jedis.incrByFloat("key", 1.5);
        assertEquals("2.5", jedis.get("key"));
    }

    @TestTemplate
    public void whenIncrementingWithIncrBy_ensureValuesAreCorrect(Jedis jedis) {
        jedis.set("key", "0");
        jedis.incrBy("key", 1);
        assertEquals("1", jedis.get("key"));
        jedis.incrBy("key", 2);
        assertEquals("3", jedis.get("key"));
    }

    @TestTemplate
    public void whenIncrementingText_ensureException(Jedis jedis) {
        jedis.set("key", "foo");
        assertThrows(JedisDataException.class, () -> jedis.incrBy("key", 1));
        assertThrows(JedisDataException.class, () -> jedis.incrByFloat("key", 1.5));
    }

    @TestTemplate
    public void decrDoesNotClearTtl(Jedis jedis) {
        String key = "mykey";
        jedis.set(key, "0");
        jedis.expire(key, 100L);

        jedis.decr(key);
        long ttl = jedis.ttl(key);

        assertTrue(ttl > 0);
    }

    @TestTemplate
    public void decrByDoesNotClearTtl(Jedis jedis) {
        String key = "mykey";
        jedis.set(key, "0");
        jedis.expire(key, 100L);

        jedis.decrBy(key, 10);
        long ttl = jedis.ttl(key);

        assertTrue(ttl > 0);
    }

    @TestTemplate
    public void testSetNXNonUTF8binary(Jedis jedis) {
        jedis.setnx("foo".getBytes(), msg);
        assertArrayEquals(msg, jedis.get("foo".getBytes()));
    }

    @TestTemplate
    public void testSetEXNonUTF8binary(Jedis jedis) {
        jedis.setex("foo".getBytes(), 100, msg);
        assertArrayEquals(msg, jedis.get("foo".getBytes()));
    }

    @TestTemplate
    public void testMsetNonUTF8binary(Jedis jedis) {
        jedis.mset("foo".getBytes(), msg);
        assertArrayEquals(msg, jedis.get("foo".getBytes()));
    }

    @TestTemplate
    public void testGetSetNonUTF8binary(Jedis jedis) {
        jedis.getSet("foo".getBytes(), msg);
        assertArrayEquals(msg, jedis.get("foo".getBytes()));
    }
}
