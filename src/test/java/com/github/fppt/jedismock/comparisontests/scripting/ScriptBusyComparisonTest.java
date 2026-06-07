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
 * Covers the {@code BUSY} reply: once a Lua script has run longer than
 * {@code lua-time-limit}, real Redis keeps serving other connections but rejects
 * ordinary commands with {@code -BUSY Redis is busy running a script...} (only
 * {@code SCRIPT KILL} / {@code SHUTDOWN NOSAVE} are accepted). This is the other
 * half of the {@code unit/multi.tcl} "MULTI and script timeout" deficiency: the
 * mock ran every command under one global lock, so while a script looped, other
 * clients blocked on that lock forever instead of getting a prompt BUSY.
 *
 * <p>The test lowers {@code lua-time-limit} (so the script becomes busy quickly),
 * starts a runaway script on one connection, and asserts another connection is
 * rejected with BUSY rather than wedged. The {@code assertTimeoutPreemptively}
 * guard turns the old blocking behaviour into a clean failure ("deadlock")
 * instead of a hung suite.
 */
@ExtendWith(ComparisonBase.class)
public class ScriptBusyComparisonTest {

    @TestTemplate
    public void busyScriptRejectsOtherClientsWithBusyError(Jedis jedis, HostAndPort hostAndPort) {
        // Lower the busy threshold so the script is flagged busy quickly.
        jedis.configSet("lua-time-limit", "100");

        Jedis busyClient = new Jedis(hostAndPort.getHost(), hostAndPort.getPort(), 1_000_000);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            pool.submit(() -> {
                try {
                    busyClient.eval("while true do end", 0);
                } catch (Exception ignored) {
                    // aborted by the SCRIPT KILL below -> error here
                }
            });

            Assertions.assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
                // Let the script exceed lua-time-limit and become "busy".
                Thread.sleep(500);
                // An ordinary command from another client must be rejected with
                // BUSY rather than blocking on the lock held by the script.
                assertThatThrownBy(() -> jedis.get("anykey"))
                        .isInstanceOf(JedisDataException.class)
                        .hasMessageContaining("BUSY");
                // SCRIPT KILL is still accepted and restores normal operation.
                jedis.scriptKill();
                assertThat(jedis.ping()).isEqualTo("PONG");
            }, TestErrorMessages.DEADLOCK_ERROR_MESSAGE);
        } finally {
            pool.shutdownNow();
            busyClient.close();
        }
    }
}
