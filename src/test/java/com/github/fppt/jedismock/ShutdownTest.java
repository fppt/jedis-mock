package com.github.fppt.jedismock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import com.github.fppt.jedismock.storage.ScriptingManager;
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
            // The runaway script is gone too, before the server counts as stopped:
            // its client lost the connection, and nothing is left to make the
            // restarted server BUSY.
            assertThat(server.getScriptingManager().isRunning()).isFalse();
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

    /**
     * {@code stop()} must not return while a script is still running: the
     * scripting manager is shared by every start of the same server, so a
     * script outliving the stop would make the restarted server reply BUSY to
     * its first commands (seen as a flaky failure on CI). A script here is
     * simulated by a thread that terminates a while after the kill arrives.
     */
    @Test
    void stopWaitsForARunningScriptToTerminate() throws Exception {
        ScriptingManager scripting = server.getScriptingManager();
        scripting.start();
        Thread scriptThread = new Thread(() -> {
            while (!scripting.isKillRequested()) {
                Thread.onSpinWait();
            }
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            scripting.stop();
        });
        scriptThread.start();

        server.stop();
        assertThat(scripting.isRunning()).isFalse();
        scriptThread.join(5_000);
    }

    private void awaitStopped() throws InterruptedException {
        for (int i = 0; i < 100 && server.isRunning(); i++) {
            Thread.sleep(50);
        }
        assertThat(server.isRunning()).withFailMessage("server still running after SHUTDOWN").isFalse();
    }
}
