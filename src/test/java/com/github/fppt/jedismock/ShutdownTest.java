package com.github.fppt.jedismock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisBusyException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code SHUTDOWN} actually stopping the server. Mock-only: against the shared
 * real-Redis container this would end every other comparison test, so the
 * replies that do not shut anything down are compared in
 * {@code ShutdownComparisonTest} instead, and the shutdown itself is checked
 * here against what real Redis does (connections dropped without a reply, the
 * port released, even an unkillable script terminated).
 */
class ShutdownTest {

    private RedisServer server;
    private String host;
    private int port;
    private final ExecutorService pool = Executors.newSingleThreadExecutor();

    @BeforeEach
    void setUp() throws IOException {
        server = RedisServer.newRedisServer().start();
        host = server.getHost();
        port = server.getBindPort();
    }

    @AfterEach
    void tearDown() throws IOException {
        pool.shutdownNow();
        if (server.isRunning()) {
            server.stop();
        }
    }

    @Test
    void shutdownStopsTheServerAndDropsTheConnection() throws Exception {
        try (Jedis jedis = new Jedis(host, port)) {
            jedis.set("k", "v");
            assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SHUTDOWN))
                    .isInstanceOf(JedisConnectionException.class);
        }
        awaitStopped();
        assertThatThrownBy(() -> new Jedis(host, port).ping())
                .isInstanceOf(JedisConnectionException.class);
    }

    @Test
    void shutdownNosaveTerminatesEvenAnUnkillableScript() throws Exception {
        Jedis busyClient = new Jedis(host, port, 1_000_000);
        try (Jedis jedis = new Jedis(host, port)) {
            jedis.configSet("lua-time-limit", "100");
            Future<Object> script = pool.submit(() ->
                    busyClient.eval("redis.call('set', KEYS[1], 'y') while true do end", 1, "k"));
            Thread.sleep(500);
            assertThatThrownBy(jedis::ping).isInstanceOf(JedisBusyException.class);
            assertThatThrownBy(jedis::scriptKill)
                    .isInstanceOf(JedisDataException.class)
                    .hasMessageStartingWith("UNKILLABLE");

            assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SHUTDOWN, "NOSAVE"))
                    .isInstanceOf(JedisConnectionException.class);
            awaitStopped();
            // The runaway script is gone too: its client lost the connection, and
            // the server can be started again on a free thread pool.
            assertThatThrownBy(() -> script.get(5, TimeUnit.SECONDS))
                    .cause().isInstanceOf(JedisConnectionException.class);
        }
        busyClient.close();
        assertThatThrownBy(() -> new Jedis(host, port).ping())
                .isInstanceOf(JedisConnectionException.class);

        server.start();
        try (Jedis jedis = new Jedis(server.getHost(), server.getBindPort())) {
            assertThat(jedis.ping()).isEqualTo("PONG");
        }
    }

    private void awaitStopped() throws InterruptedException {
        for (int i = 0; i < 100 && server.isRunning(); i++) {
            Thread.sleep(50);
        }
        assertThat(server.isRunning()).withFailMessage("server still running after SHUTDOWN").isFalse();
    }
}
