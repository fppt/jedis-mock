package com.github.fppt.jedismock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import com.github.fppt.jedismock.commands.RedisCommandParser;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.server.MockExecutor;
import com.github.fppt.jedismock.server.RedisOperationExecutor;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.ServiceOptions;
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
import java.util.List;
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
            // The asynchronous shutdown must have run its course on the old
            // server only: it ends, and the restarted one keeps accepting.
            for (int i = 0; i < 40 && shutdownThreadAlive(); i++) {
                Thread.sleep(50);
            }
            assertThat(shutdownThreadAlive()).withFailMessage("shutdown thread still alive").isFalse();
            try (Jedis again = new Jedis(server.getHost(), server.getBindPort())) {
                assertThat(again.ping()).isEqualTo("PONG");
            }
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
        // The same goes for the commands served without the data lock: a stale
        // SHUTDOWN or KILL of a dead client must not hit a (restarted) server.
        assertThat(executor.execCommand(RedisCommandParser.parse(array("SCRIPT", "KILL")))).isEqualTo(Response.SKIP);
        assertThat(executor.execCommand(RedisCommandParser.parse(array("SHUTDOWN", "NOSAVE")))).isEqualTo(Response.SKIP);
        Thread.sleep(200);
        assertThat(server.isRunning()).isTrue();
    }

    /**
     * A client task that is merely paused (descheduled) in the middle of
     * dispatching a command survives the closing of its socket and the
     * interrupt of its pool. If {@code stop()} returned while it is paused, it
     * could resume against a restarted server, e.g. with a stale SHUTDOWN.
     * {@code stop()} must therefore wait for every client task to terminate.
     * The pause is simulated by an interceptor that parks the first PING.
     */
    @Test
    void stopWaitsForPausedClientTasks() throws Exception {
        server.stop();
        CountDownLatch pingParked = new CountDownLatch(1);
        CountDownLatch releasePing = new CountDownLatch(1);
        server.setOptions(ServiceOptions.withInterceptor((state, name, params) -> {
            if ("ping".equals(name) && pingParked.getCount() > 0) {
                pingParked.countDown();
                // A descheduled thread cannot be interrupted out of its pause,
                // so neither can this simulation of one.
                boolean released = false;
                boolean interrupted = false;
                while (!released) {
                    try {
                        releasePing.await();
                        released = true;
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            return MockExecutor.proceed(state, name, params);
        }));
        server.start();
        Jedis parked = new Jedis(server.getHost(), server.getBindPort(), 1_000_000);
        Future<?> ping = pool.submit(() -> parked.ping());
        assertThat(pingParked.await(5, TimeUnit.SECONDS)).isTrue();

        CountDownLatch stopped = new CountDownLatch(1);
        Thread stopper = new Thread(() -> {
            try {
                server.stop();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            stopped.countDown();
        });
        stopper.setDaemon(true);
        stopper.start();
        try {
            assertThat(stopped.await(500, TimeUnit.MILLISECONDS))
                    .withFailMessage("stop() returned while a client task was still paused").isFalse();
        } finally {
            releasePing.countDown();
        }
        assertThat(stopped.await(5, TimeUnit.SECONDS)).withFailMessage("stop() did not return").isTrue();
        assertThat(clientThreadsAlive()).isZero();
        assertThatThrownBy(() -> ping.get(5, TimeUnit.SECONDS)).cause().isInstanceOf(JedisConnectionException.class);
        parked.close();
    }

    /**
     * Two {@code SHUTDOWN}s in flight at once both stop the same generation of
     * the server. The one that runs late, possibly after the caller restarted
     * the server, must not act on the new generation: it must neither arm the
     * kill-on-start flag against it nor publish it as stopped (which would make
     * the next {@code start()} fail to bind the port that is still open). The
     * requester of a shutdown request is a client of the generation it was
     * sent to, and that generation closed it when it stopped, so a request
     * from a closed client is stale.
     */
    @Test
    void staleShutdownRequestIsIgnored() throws Exception {
        RedisClient staleRequester = new RedisClient(server, Mockito.mock(Socket.class), c -> { }, server.currentGeneration());
        server.stop();
        server.start();

        staleRequester.shutdownServer();
        Thread.sleep(300);
        assertThat(server.isRunning()).isTrue();
        ScriptingManager scripting = server.getScriptingManager();
        scripting.start();
        assertThat(scripting.isKillRequested()).isFalse();
        scripting.stop();
        try (Jedis jedis = new Jedis(server.getHost(), server.getBindPort())) {
            assertThat(jedis.ping()).isEqualTo("PONG");
        }
    }

    /**
     * Whether a request is stale is decided by the generation it was sent to,
     * not by the state of its connection: a peer may hang up right after
     * sending SHUTDOWN, closing the requester before the shutdown thread gets
     * going, and that must not cancel the only shutdown request there is.
     */
    @Test
    void shutdownRequestedByAPeerThatHungUpStillStops() throws Exception {
        RedisClient requester = new RedisClient(server, Mockito.mock(Socket.class), c -> { }, server.currentGeneration());
        requester.close();

        requester.shutdownServer();
        awaitStopped();
    }

    /**
     * A client task blocked on the data lock can only observe that its client
     * is closed, and exit, once it gets that lock. {@code stop()} must
     * therefore not hold the lock while it waits for the client tasks to
     * terminate, or every such task would force the wait to its timeout and
     * the server would be published as stopped with the task still alive.
     * The task is parked by an interceptor until {@code stop()} is already
     * waiting at the barrier, so that it queues on the lock behind it.
     */
    @Test
    void stopDoesNotHoldTheDataLockWhileAwaitingClientTasks() throws Exception {
        server.stop();
        CountDownLatch setParked = new CountDownLatch(1);
        CountDownLatch releaseSet = new CountDownLatch(1);
        server.setOptions(ServiceOptions.withInterceptor((state, name, params) -> {
            if ("set".equals(name) && setParked.getCount() > 0) {
                setParked.countDown();
                boolean released = false;
                boolean interrupted = false;
                while (!released) {
                    try {
                        releaseSet.await();
                        released = true;
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            return MockExecutor.proceed(state, name, params);
        }));
        server.start();
        Jedis parked = new Jedis(server.getHost(), server.getBindPort(), 1_000_000);
        Future<?> set = pool.submit(() -> parked.set("k", "v"));
        assertThat(setParked.await(5, TimeUnit.SECONDS)).isTrue();

        CountDownLatch stopped = new CountDownLatch(1);
        Thread stopper = new Thread(() -> {
            try {
                server.stop();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            stopped.countDown();
        });
        stopper.setDaemon(true);
        long elapsedMillis;
        synchronized (server.getRedisBases()) {
            stopper.start();
            // stop() is now blocked at the data-lock barrier behind this thread;
            // let the parked SET queue on the same lock, behind stop().
            releaseSet.countDown();
            Thread.sleep(300);
            elapsedMillis = System.currentTimeMillis();
        }
        assertThat(stopped.await(5, TimeUnit.SECONDS)).withFailMessage("stop() did not return promptly").isTrue();
        elapsedMillis = System.currentTimeMillis() - elapsedMillis;
        assertThat(elapsedMillis).isLessThan(5_000L);
        assertThat(clientThreadsAlive()).isZero();
        assertThatThrownBy(() -> set.get(5, TimeUnit.SECONDS)).cause().isInstanceOf(JedisConnectionException.class);
        parked.close();
    }

    @Test
    void concurrentShutdownsStopTheServerOnceAndLeaveARestartAlone() throws Exception {
        Jedis first = new Jedis(host, port);
        Jedis second = new Jedis(host, port);
        ExecutorService shooters = Executors.newFixedThreadPool(2);
        try {
            CountDownLatch go = new CountDownLatch(1);
            Future<?> a = shooters.submit(() -> {
                go.await();
                return first.sendCommand(Protocol.Command.SHUTDOWN, "NOSAVE");
            });
            Future<?> b = shooters.submit(() -> {
                go.await();
                return second.sendCommand(Protocol.Command.SHUTDOWN, "NOSAVE");
            });
            go.countDown();
            for (Future<?> f : List.of(a, b)) {
                assertThatThrownBy(() -> f.get(5, TimeUnit.SECONDS)).cause().isInstanceOf(JedisConnectionException.class);
            }
        } finally {
            shooters.shutdownNow();
            first.close();
            second.close();
        }
        awaitStopped();

        server.start();
        for (int i = 0; i < 40 && shutdownThreadAlive(); i++) {
            Thread.sleep(50);
        }
        assertThat(shutdownThreadAlive()).withFailMessage("shutdown thread still alive").isFalse();
        assertThat(server.isRunning()).isTrue();
        ScriptingManager scripting = server.getScriptingManager();
        scripting.start();
        assertThat(scripting.isKillRequested()).isFalse();
        scripting.stop();
        try (Jedis jedis = new Jedis(server.getHost(), server.getBindPort())) {
            assertThat(jedis.ping()).isEqualTo("PONG");
        }
    }

    private static long clientThreadsAlive() {
        return Thread.getAllStackTraces().entrySet().stream()
                .filter(e -> e.getKey().isAlive())
                .filter(e -> java.util.Arrays.stream(e.getValue())
                        .anyMatch(f -> RedisClient.class.getName().equals(f.getClassName()) && "run".equals(f.getMethodName())))
                .count();
    }

    private static String array(String... words) {
        StringBuilder sb = new StringBuilder("*").append(words.length).append("\r\n");
        for (String word : words) {
            sb.append('$').append(word.length()).append("\r\n").append(word).append("\r\n");
        }
        return sb.toString();
    }

    private static boolean shutdownThreadAlive() {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(t -> t.isAlive() && "jedis-mock-shutdown".equals(t.getName()));
    }

    private void awaitStopped() throws InterruptedException {
        for (int i = 0; i < 100 && server.isRunning(); i++) {
            Thread.sleep(50);
        }
        assertThat(server.isRunning()).withFailMessage("server still running after SHUTDOWN").isFalse();
    }
}
