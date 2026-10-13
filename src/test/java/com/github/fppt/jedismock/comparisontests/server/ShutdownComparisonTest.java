package com.github.fppt.jedismock.comparisontests.server;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisBusyException;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The replies of {@code SHUTDOWN} that do <em>not</em> shut the server down:
 * argument validation, the transaction rule, and the {@code -BUSY} refusal of
 * a shutdown that would save while a script is timing out. An actual shutdown
 * would take the shared container with it, so that part lives in the mock-only
 * {@code ShutdownTest}.
 */
@ExtendWith(ComparisonBase.class)
public class ShutdownComparisonTest {

    private static final String BUSY_SCRIPT_MESSAGE =
            "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.";
    private static final String BUSY_FUNCTION_MESSAGE =
            "BUSY Redis is busy running a script. You can only call FUNCTION KILL or SHUTDOWN NOSAVE.";

    /** Spin for about 1.5 seconds using only the read-only TIME command, then return 'done'. */
    private static final String SPIN =
            "local function now() local t = redis.call('time') return tonumber(t[1]) * 1000000 + tonumber(t[2]) end "
                    + "local deadline = now() + 1500000 "
                    + "while now() < deadline do end "
                    + "return 'done'";

    private Jedis busyClient;
    private ExecutorService pool;

    @BeforeEach
    void setUp(Jedis jedis, HostAndPort hostAndPort) {
        jedis.flushAll();
        jedis.functionFlush();
        jedis.configSet("lua-time-limit", "100");
        busyClient = new Jedis(hostAndPort.getHost(), hostAndPort.getPort(), 1_000_000);
        pool = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void tearDown() {
        pool.shutdownNow();
        busyClient.close();
    }

    @TestTemplate
    public void unknownOptionIsASyntaxError(Jedis jedis) {
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SHUTDOWN, "foo"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR syntax error");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SHUTDOWN, "NOSAVE", "foo"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR syntax error");
    }

    @TestTemplate
    public void saveAndNosaveAreMutuallyExclusive(Jedis jedis) {
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SHUTDOWN, "NOSAVE", "SAVE"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR syntax error");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SHUTDOWN, "save", "nosave"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR syntax error");
    }

    @TestTemplate
    public void abortMustBeAlone(Jedis jedis) {
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SHUTDOWN, "NOSAVE", "ABORT"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR syntax error");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SHUTDOWN, "ABORT", "NOW"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR syntax error");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SHUTDOWN, "NOW", "FORCE", "ABORT"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR syntax error");
    }

    @TestTemplate
    public void abortWithNothingToAbort(Jedis jedis) {
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SHUTDOWN, "ABORT"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR No shutdown in progress.");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SHUTDOWN, "abort"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR No shutdown in progress.");
    }

    @TestTemplate
    public void shutdownIsNotAllowedInsideTransaction(Jedis jedis) {
        jedis.sendCommand(Protocol.Command.MULTI);
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SHUTDOWN, "NOSAVE"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Command not allowed inside a transaction");
        assertThat((byte[]) jedis.sendCommand(Protocol.Command.SET, "foo", "bar")).asString().isEqualTo("QUEUED");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.EXEC))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("EXECABORT Transaction discarded because of previous errors.");
        assertThat(jedis.get("foo")).isNull();
    }

    @TestTemplate
    public void shutdownThatWouldSaveIsRefusedWhileScriptIsBusy(Jedis jedis) throws Exception {
        Future<Object> result = runBusy(c -> c.eval(SPIN, 0));

        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SHUTDOWN))
                .isInstanceOf(JedisBusyException.class)
                .hasMessage(BUSY_SCRIPT_MESSAGE);
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SHUTDOWN, "SAVE"))
                .isInstanceOf(JedisBusyException.class)
                .hasMessage(BUSY_SCRIPT_MESSAGE);
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SHUTDOWN, "NOW", "FORCE"))
                .isInstanceOf(JedisBusyException.class)
                .hasMessage(BUSY_SCRIPT_MESSAGE);
        // Argument validation still comes first.
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SHUTDOWN, "NOSAVE", "ABORT"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR syntax error");

        assertThat(result.get(10, TimeUnit.SECONDS)).isEqualTo("done");
        assertThat(jedis.ping()).isEqualTo("PONG");
    }

    @TestTemplate
    public void shutdownThatWouldSaveIsRefusedWhileFunctionIsBusy(Jedis jedis) throws Exception {
        jedis.functionLoad("#!lua name=lib\nredis.register_function('spin', function(KEYS, ARGV) " + SPIN + " end)\n");
        Future<Object> result = runBusy(c -> c.fcall("spin", List.of(), List.of()));

        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SHUTDOWN))
                .isInstanceOf(JedisBusyException.class)
                .hasMessage(BUSY_FUNCTION_MESSAGE);

        assertThat(result.get(10, TimeUnit.SECONDS)).isEqualTo("done");
        assertThat(jedis.ping()).isEqualTo("PONG");
    }

    private Future<Object> runBusy(Function<Jedis, Object> script) throws InterruptedException {
        Future<Object> result = pool.submit(() -> script.apply(busyClient));
        Thread.sleep(500);
        return result;
    }
}
