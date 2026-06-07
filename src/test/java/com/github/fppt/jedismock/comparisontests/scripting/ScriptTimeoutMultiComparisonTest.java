package com.github.fppt.jedismock.comparisontests.scripting;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import com.github.fppt.jedismock.comparisontests.TestErrorMessages;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Faithful port of the {@code unit/multi.tcl} test "MULTI and script timeout".
 *
 * <p>A runaway script is started on one connection; meanwhile another connection
 * opens a {@code MULTI}, queues an {@code INCR} (which is rejected with BUSY
 * because a script is timing out, dirtying the transaction), the script is
 * killed, a second {@code INCR} is queued, and {@code EXEC} must then abort with
 * {@code EXECABORT ... previous errors}. The counter must be left untouched and
 * the connection must no longer be in MULTI state.
 *
 * <p>Raw {@code sendCommand} is used (rather than Jedis's {@code Transaction}
 * helper) to mirror the tcl deferring/raw clients and to keep full control over
 * which replies are errors. Each step that the tcl wraps in {@code catch} is
 * wrapped here in a try/catch for the same reason.
 *
 * <p>This exercises the interplay of the BUSY gate with transactions: data
 * commands rejected with BUSY during {@code MULTI} dirty the transaction, while
 * the transaction-control commands themselves are still serviced.
 */
@ExtendWith(ComparisonBase.class)
public class ScriptTimeoutMultiComparisonTest {

    @TestTemplate
    public void multiAndScriptTimeout(Jedis jedis, HostAndPort hostAndPort) {
        jedis.configSet("lua-time-limit", "50");
        jedis.set("xx", "1");

        Jedis rd1 = new Jedis(hostAndPort.getHost(), hostAndPort.getPort(), 1_000_000);
        Jedis r2 = new Jedis(hostAndPort.getHost(), hostAndPort.getPort(), 1_000_000);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            pool.submit(() -> {
                try {
                    rd1.eval("while true do end", 0);
                } catch (Exception ignored) {
                    // aborted by SCRIPT KILL below
                }
            });

            Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
                Thread.sleep(300); // let the script exceed lua-time-limit

                // MULTI arrives during the timeout: it is serviced (the tcl
                // comment allows "refused, or allowed to pass").
                ignoringErrors(() -> r2.sendCommand(Protocol.Command.MULTI));
                // INCR is a data command -> rejected with BUSY -> dirties the tx.
                ignoringErrors(() -> r2.sendCommand(Protocol.Command.INCR, "xx"));

                jedis.scriptKill();
                Thread.sleep(300); // give the script time to actually abort

                ignoringErrors(() -> r2.sendCommand(Protocol.Command.INCR, "xx"));

                String execError = null;
                try {
                    r2.sendCommand(Protocol.Command.EXEC);
                } catch (JedisDataException e) {
                    execError = e.getMessage();
                }
                assertThat(execError)
                        .as("EXEC must abort because a command errored while queued")
                        .contains("EXECABORT")
                        .contains("previous errors");

                // The transaction ran wholly or not at all (we expect not at all).
                assertThat(jedis.get("xx")).isIn("1", "3");

                // The connection is no longer in MULTI state: a PING with an
                // argument echoes it back rather than returning QUEUED.
                assertThat(r2.ping("asdf")).isEqualTo("asdf");
            }, TestErrorMessages.DEADLOCK_ERROR_MESSAGE);
        } finally {
            pool.shutdownNow();
            rd1.close();
            r2.close();
        }
    }

    private static void ignoringErrors(Runnable command) {
        try {
            command.run();
        } catch (JedisDataException ignored) {
            // mirrors the tcl `catch {...}`: BUSY / queue errors are expected
        }
    }
}
