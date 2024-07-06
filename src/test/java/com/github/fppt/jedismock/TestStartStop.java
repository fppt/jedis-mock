package com.github.fppt.jedismock;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class TestStartStop {
    @Test
    void testMultipleServerReuse() throws IOException {
        Set<String> oldThreads = threadNames();
        RedisServer server = RedisServer.newRedisServer();
        for (int i = 0; i < 2000; i++) {
            startCheckStop(server);
        }
        Set<String> newThreads = threadNames();
        newThreads.removeAll(oldThreads);
        Assertions.assertThat(newThreads).hasSizeLessThan(10);
    }

    @Test
    void testMultipleServerCreation() throws IOException {
        Set<String> oldThreads = threadNames();
        for (int i = 0; i < 2000; i++) {
            RedisServer server = RedisServer.newRedisServer();
            startCheckStop(server);
        }
        Set<String> newThreads = threadNames();
        newThreads.removeAll(oldThreads);
        Assertions.assertThat(newThreads).hasSizeLessThan(10);
    }

    @Test
    void testStartAlreadyStarted() throws IOException {
        RedisServer server = RedisServer.newRedisServer();
        server.start();
        try {
            Assertions.assertThatThrownBy(server::start).isInstanceOf(IllegalStateException.class);
        } finally {
            server.stop();
        }
    }

    @Test
    void testStopNotStarted() {
        RedisServer server = RedisServer.newRedisServer();
        Assertions.assertThatThrownBy(server::stop).hasMessageContaining("is not running");
    }

    private static Set<String> threadNames() {
        return Thread.getAllStackTraces()
                .keySet()
                .stream()
                .map(Thread::getName)
                .collect(Collectors.toCollection(HashSet::new));
    }

    private static void startCheckStop(RedisServer server) throws IOException {
        Assertions.assertThat(server.isRunning()).isFalse();
        server.start();
        Assertions.assertThat(server.isRunning()).isTrue();
        try (Jedis jedis = new Jedis(server.getHost(), server.getBindPort())) {
            Assertions.assertThat(jedis.ping()).isEqualTo("PONG");
        }
        server.stop();
        Assertions.assertThat(server.isRunning()).isFalse();
    }

}
