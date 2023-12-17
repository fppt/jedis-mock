package com.github.fppt.jedismock;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RList;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRedissonConnection {
    private static RedisServer redisServer;
    private static RedissonClient client;

    @BeforeAll
    static void setUp() throws IOException {
        redisServer = RedisServer.newRedisServer();
        redisServer.start();
        Config config = new Config();
        config.useSingleServer().setAddress(
                String.format("redis://%s:%d", redisServer.getHost(), redisServer.getBindPort()));
        client = Redisson.create(config);
    }

    @AfterAll
    static void tearDown() throws IOException {
        redisServer.stop();
    }

    @Test
    public void testStringMap() {
        RMap<String, String> map = client.getMap("stringMap");
        assertThat(map.put("foo", "bar")).isNull();
        assertThat(map.get("foo")).isEqualTo("bar");
        assertThat(map.put("foo", "baz")).isEqualTo("bar");
    }

    @Test
    public void testIntegerMap() {
        RMap<String, Integer> map = client.getMap("intMap");
        assertThat(map.put("foo", 42)).isNull();
        assertThat(map.get("foo")).isEqualTo(42);
        assertThat(map.put("foo", 43)).isEqualTo(42);
    }

    @Test
    public void testIntList() {
        RList<Integer> list = client.getList("intList");
        assertThat(list.add(11)).isTrue();
        assertThat(list.add(15)).isTrue();
        assertThat(list.readAll()).containsExactly(11, 15);
    }

    @Test
    public void testLock() {
        String key = "an-example-key";
        RLock rLock = client.getLock(key);
        rLock.lock();
        assertThat(rLock.isLocked()).isTrue();
        rLock.unlock();
        assertThat(rLock.isLocked()).isFalse();
    }
}
