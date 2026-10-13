package com.github.fppt.jedismock.comparisontests.scripting;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
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
 * Once a timed-out script has executed a write command, real Redis refuses to
 * kill it: {@code SCRIPT KILL} and {@code FUNCTION KILL} reply {@code -UNKILLABLE}
 * and the server stays {@code -BUSY} until the script ends by itself. The flag
 * is set when a write command is dispatched, after its arity check and before
 * it runs, so a write rejected for arity leaves the script killable while a
 * write that fails at execution does not.
 *
 * <p>Every busy script here spins for a bounded time (polling {@code TIME}) so an
 * unkillable one never leaves the shared server stuck.
 */
@ExtendWith(ComparisonBase.class)
public class UnkillableScriptComparisonTest {

    private static final String UNKILLABLE_MESSAGE = "UNKILLABLE Sorry the script already executed write commands "
            + "against the dataset. You can either wait the script termination or kill the server in a hard way "
            + "using the SHUTDOWN NOSAVE command.";
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
        // Lower the busy threshold so the kill commands are permitted quickly.
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
    public void scriptThatWroteCannotBeKilled(Jedis jedis) throws Exception {
        Future<Object> result = runBusy(c -> c.eval("redis.call('set', KEYS[1], 'y') " + SPIN, List.of("k"), List.of()));

        assertBusy(jedis, BUSY_SCRIPT_MESSAGE);
        assertThatThrownBy(jedis::scriptKill)
                .isInstanceOf(JedisDataException.class)
                .hasMessage(UNKILLABLE_MESSAGE);
        // The refused kill leaves the script running and the server busy.
        assertBusy(jedis, BUSY_SCRIPT_MESSAGE);

        assertThat(result.get(10, TimeUnit.SECONDS)).isEqualTo("done");
        assertThat(jedis.ping()).isEqualTo("PONG");
        assertThat(jedis.get("k")).isEqualTo("y");
    }

    @TestTemplate
    public void writeRejectedForArityLeavesScriptKillable(Jedis jedis) throws Exception {
        String script = "redis.pcall('del') " + SPIN;
        String sha = jedis.scriptLoad(script);
        Future<Object> result = runBusy(c -> c.eval(script, 0));

        assertBusy(jedis, BUSY_SCRIPT_MESSAGE);
        // DEL never ran (wrong number of arguments), so nothing was written.
        assertThat(jedis.scriptKill()).isEqualTo("OK");
        assertThat(jedis.ping()).isEqualTo("PONG");

        assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS))
                .cause()
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Script killed by user with SCRIPT KILL... script: " + sha + ", on @user_script:1.");
    }

    @TestTemplate
    public void writeThatFailsAtExecutionStillMakesScriptUnkillable(Jedis jedis) throws Exception {
        jedis.set("k", "abc");
        Future<Object> result = runBusy(c -> c.eval("redis.pcall('incr', KEYS[1]) " + SPIN, List.of("k"), List.of()));

        assertBusy(jedis, BUSY_SCRIPT_MESSAGE);
        // INCR was dispatched (and failed on a non-integer value): the script already counts as writing.
        assertThatThrownBy(jedis::scriptKill)
                .isInstanceOf(JedisDataException.class)
                .hasMessage(UNKILLABLE_MESSAGE);
        assertBusy(jedis, BUSY_SCRIPT_MESSAGE);

        assertThat(result.get(10, TimeUnit.SECONDS)).isEqualTo("done");
        assertThat(jedis.get("k")).isEqualTo("abc");
    }

    @TestTemplate
    public void functionThatWroteCannotBeKilledByEitherCommand(Jedis jedis) throws Exception {
        jedis.functionLoad("#!lua name=lib\n"
                + "redis.register_function('w', function(KEYS, ARGV) redis.call('set', KEYS[1], 'z') " + SPIN + " end)\n");
        Future<Object> result = runBusy(c -> c.fcall("w", List.of("k"), List.of()));

        assertBusy(jedis, BUSY_FUNCTION_MESSAGE);
        // The write check comes before the script-vs-function check, so even the
        // mismatching SCRIPT KILL reports UNKILLABLE rather than BUSY.
        assertThatThrownBy(jedis::scriptKill)
                .isInstanceOf(JedisDataException.class)
                .hasMessage(UNKILLABLE_MESSAGE);
        assertThatThrownBy(jedis::functionKill)
                .isInstanceOf(JedisDataException.class)
                .hasMessage(UNKILLABLE_MESSAGE);
        assertBusy(jedis, BUSY_FUNCTION_MESSAGE);

        assertThat(result.get(10, TimeUnit.SECONDS)).isEqualTo("done");
        assertThat(jedis.ping()).isEqualTo("PONG");
        assertThat(jedis.get("k")).isEqualTo("z");
    }

    @TestTemplate
    public void writeFlagDoesNotLeakIntoTheNextScript(Jedis jedis) throws Exception {
        Future<Object> first = runBusy(c -> c.eval("redis.call('set', KEYS[1], 'y') " + SPIN, List.of("k"), List.of()));
        assertBusy(jedis, BUSY_SCRIPT_MESSAGE);
        assertThatThrownBy(jedis::scriptKill).hasMessage(UNKILLABLE_MESSAGE);
        assertThat(first.get(10, TimeUnit.SECONDS)).isEqualTo("done");

        // A read-only script started afterwards must be killable again.
        Future<Object> second = runBusy(c -> c.eval(SPIN, 0));
        assertBusy(jedis, BUSY_SCRIPT_MESSAGE);
        assertThat(jedis.scriptKill()).isEqualTo("OK");
        assertThat(jedis.ping()).isEqualTo("PONG");
        assertThatThrownBy(() -> second.get(10, TimeUnit.SECONDS))
                .cause()
                .isInstanceOf(JedisDataException.class)
                .hasMessageStartingWith("ERR Script killed by user with SCRIPT KILL...");
    }

    /**
     * Starts the script on the second connection and waits until it has
     * exceeded lua-time-limit, so that the server is busy when we return.
     */
    private Future<Object> runBusy(Function<Jedis, Object> script) throws InterruptedException {
        Future<Object> result = pool.submit(() -> script.apply(busyClient));
        Thread.sleep(500);
        return result;
    }

    private static void assertBusy(Jedis jedis, String message) {
        assertThatThrownBy(jedis::ping)
                .isInstanceOf(JedisBusyException.class)
                .hasMessage(message);
    }
}
