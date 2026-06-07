package com.github.fppt.jedismock.comparisontests.scripting;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import com.github.fppt.jedismock.comparisontests.TestErrorMessages;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers {@code SCRIPT KILL}, which the mock previously did not implement at all.
 *
 * <p>This is the deficiency behind the hang of the {@code unit/multi.tcl} test
 * "MULTI and script timeout" (and its "EXEC / MULTI-EXEC body and script
 * timeout" siblings): those tests start a runaway
 * {@code eval {while true do end} 0} and then issue {@code SCRIPT KILL} to abort
 * it. The mock executes every command — {@code EVAL} included — under a single
 * global lock, so a runaway script wedged the whole server and nothing could
 * stop it.
 *
 * <p>The fix runs {@code SCRIPT KILL} on a lock-free path (it must take effect
 * while the runaway script still holds the lock) and aborts the script through a
 * LuaJ per-instruction hook. Each test asserts real-Redis behaviour, so both the
 * "real" and "mock" variants pass.
 */
@ExtendWith(ComparisonBase.class)
public class ScriptKillComparisonTest {

    /**
     * {@code SCRIPT KILL} with nothing running is a defined Redis command: it
     * replies {@code -NOTBUSY No scripts in execution right now.} Fast and
     * deterministic — no script involved.
     */
    @TestTemplate
    public void scriptKillOnIdleServerReportsNotBusy(Jedis jedis) {
        assertThatThrownBy(jedis::scriptKill)
                .isInstanceOf(JedisDataException.class)
                .hasMessageContaining("NOTBUSY");
    }

    /**
     * A runaway script must not wedge the whole server: {@code SCRIPT KILL} from
     * another connection terminates it and the server stays responsive. The
     * {@code assertTimeoutPreemptively} guard turns a regression (the old
     * deadlock) into a clean failure instead of a hung suite.
     */
    @TestTemplate
    public void scriptKillTerminatesRunawayScript(Jedis jedis, HostAndPort hostAndPort) {
        // Lower the busy threshold so SCRIPT KILL is permitted quickly.
        jedis.configSet("lua-time-limit", "100");

        Jedis busyClient = new Jedis(hostAndPort.getHost(), hostAndPort.getPort(), 1_000_000);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            pool.submit(() -> {
                try {
                    busyClient.eval("while true do end", 0);
                } catch (Exception ignored) {
                    // the script is aborted by SCRIPT KILL -> error here
                }
            });

            Assertions.assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
                // Let the script start and (on real Redis) become flagged busy.
                Thread.sleep(500);
                // Returns OK and unblocks the runaway script.
                jedis.scriptKill();
                // The server must remain responsive afterwards.
                assertThat(jedis.ping()).isEqualTo("PONG");
            }, TestErrorMessages.DEADLOCK_ERROR_MESSAGE);
        } finally {
            pool.shutdownNow();
            busyClient.close();
        }
    }
}
