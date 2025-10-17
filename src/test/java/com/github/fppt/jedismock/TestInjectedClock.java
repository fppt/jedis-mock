package com.github.fppt.jedismock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;

public class TestInjectedClock {
    RedisServer server;
    Jedis jedis;


    @BeforeEach
    void setup() throws IOException {
        server = RedisServer.newRedisServer();
        server.start();
        jedis = new Jedis(server.getHost(), server.getBindPort());
    }

    @AfterEach
    void tearDown() throws IOException {
        jedis.close();
        server.stop();
    }

    @Test
    public void testTime() throws InterruptedException {
        server.setClock(Clock.fixed(Instant.ofEpochMilli(123456789123L), ZoneId.systemDefault()));
        Thread.sleep(100);
        assertThat(jedis.time()).containsExactly("123456789", "123000");
    }

    @Test
    public void testExpireTimeForFixedClock() throws InterruptedException {
        server.setClock(Clock.fixed(Instant.ofEpochMilli(777777777000L), ZoneId.systemDefault()));
        jedis.setex("foo", 1, "bar");
        assertThat(jedis.expireTime("foo")).isEqualTo(777777778L);
        Thread.sleep(1500);
        //The key does not expire as the time is frozen
        assertThat(jedis.exists("foo")).isTrue();
    }

    @Test
    public void setTimeInFuture() {
        jedis.setex("key1", 20, "v1");
        jedis.setex("key2", 40, "v2");
        server.setClock(Clock.offset(
                server.getClock(), Duration.ofSeconds(30)
        ));
        //Skipped 30 seconds: key1 key must be expired, key2 -- not yet
        assertThat(jedis.exists("key1")).isFalse();
        assertThat(jedis.exists("key2")).isTrue();
        server.setClock(Clock.offset(
                server.getClock(), Duration.ofSeconds(30)
        ));
        //Skipped another 30 seconds: both keys expired
        assertThat(jedis.exists("key1")).isFalse();
        assertThat(jedis.exists("key2")).isFalse();
    }

    @Test
    public void setTimeInThePast() throws InterruptedException {
        jedis.setex("key1", 1, "v1");
        server.setClock(Clock.offset(
                server.getClock(), Duration.ofSeconds(-2)
        ));
        Thread.sleep(1500);
        assertThat(jedis.exists("key1")).isTrue();
        Thread.sleep(1600);
        assertThat(jedis.exists("key1")).isFalse();
    }

    @Test
    void freezeKey() throws InterruptedException {
        jedis.setex("key1", 1, "v1");
        server.setClock(Clock.fixed(Instant.now(), ZoneId.systemDefault()));
        Thread.sleep(1500);
        //The key must have expired, the time is stopped
        assertThat(jedis.exists("key1")).isTrue();
    }
}
