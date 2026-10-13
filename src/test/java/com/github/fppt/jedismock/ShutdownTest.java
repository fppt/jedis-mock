package com.github.fppt.jedismock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import com.github.fppt.jedismock.commands.RedisCommandParser;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.server.RedisOperationExecutor;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.ScriptingManager;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisBusyException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
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
     * its first commands (seen as a flaky failure on CI). A script is
     * simulated by a thread that, like a real script thread, holds the data
     * lock for its whole run and only terminates a while after the kill arrives.
     */
    @Test
    void stopWaitsForARunningScriptToTerminate() throws Exception {
        ScriptingManager scripting = server.getScriptingManager();
        CountDownLatch started = new CountDownLatch(1);
        Thread scriptThread = new Thread(() -> {
            synchronized (server.getRedisBases()) {
                scripting.start();
                started.countDown();
                while (!scripting.isKillRequested()) {
                    Thread.onSpinWait();
                }
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                scripting.stop();
            }
        });
        scriptThread.setDaemon(true);
        scriptThread.start();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        try {
            server.stop();
            assertThat(scripting.isRunning()).isFalse();
        } finally {
            // Should the kill never arrive, do not leave the helper spinning.
            scripting.beginShutdown();
            scriptThread.join(5_000);
        }
    }

    /**
     * A command that already passed the busy gate when the server stops is
     * blocked on the data lock behind the dying script and ignores the
     * interrupt from the client pool; if it is an EVAL, it would start a fresh
     * script whose {@code start()} clears the kill request, so it could run
     * forever on the stopped (or restarted) server. {@code stop()} must
     * therefore not return before every in-flight command has finished, and a
     * script that starts while the server is stopping must be killed at once.
     * Here the test thread plays the in-flight command by holding the data
     * lock itself.
     */
    @Test
    void stopWaitsForInFlightCommandsAndKillsScriptsStartedMeanwhile() throws Exception {
        ScriptingManager scripting = server.getScriptingManager();
        CountDownLatch stopped = new CountDownLatch(1);
        AtomicReference<Throwable> stopFailure = new AtomicReference<>();
        Thread stopper = new Thread(() -> {
            try {
                server.stop();
            } catch (Throwable t) {
                stopFailure.set(t);
            }
            stopped.countDown();
        });
        stopper.setDaemon(true);

        synchronized (server.getRedisBases()) {
            stopper.start();
            assertThat(stopped.await(500, TimeUnit.MILLISECONDS))
                    .withFailMessage("stop() returned while a command still held the data lock").isFalse();
            // The in-flight command turns out to be an EVAL: it is killed before its first instruction.
            scripting.start();
            assertThat(scripting.isKillRequested()).isTrue();
            scripting.stop();
        }
        assertThat(stopped.await(5, TimeUnit.SECONDS)).withFailMessage("stop() did not return").isTrue();
        assertThat(stopFailure.get()).isNull();
        assertThat(server.isRunning()).isFalse();

        // Once restarted, scripts run normally again.
        server.start();
        scripting.start();
        assertThat(scripting.isKillRequested()).isFalse();
        scripting.stop();
    }

    /**
     * A command still waiting for the data lock when its client is closed
     * (by the server stopping, or the client going away) is dropped rather
     * than executed, like a command left unprocessed in a disconnected
     * client's query buffer in real Redis.
     */
    @Test
    void commandsOfAClosedClientAreDropped() throws Exception {
        RedisClient client = new RedisClient(server, Mockito.mock(Socket.class), c -> { });
        OperationExecutorState state = new OperationExecutorState(client, server.getRedisBases());
        RedisOperationExecutor executor = new RedisOperationExecutor(state);
        assertThat(executor.execCommand(RedisCommandParser.parse(array("SET", "k", "v")))).isEqualTo(Response.OK);

        client.close();
        assertThat(executor.execCommand(RedisCommandParser.parse(array("SET", "k", "w")))).isEqualTo(Response.SKIP);
        assertThat(server.getRedisBases().get(0).getRMString(Slice.create("k")).getStoredDataAsString()).isEqualTo("v");
    }

    private static String array(String... words) {
        StringBuilder sb = new StringBuilder("*").append(words.length).append("\r\n");
        for (String word : words) {
            sb.append('$').append(word.length()).append("\r\n").append(word).append("\r\n");
        }
        return sb.toString();
    }

    private void awaitStopped() throws InterruptedException {
        for (int i = 0; i < 100 && server.isRunning(); i++) {
            Thread.sleep(50);
        }
        assertThat(server.isRunning()).withFailMessage("server still running after SHUTDOWN").isFalse();
    }
}
