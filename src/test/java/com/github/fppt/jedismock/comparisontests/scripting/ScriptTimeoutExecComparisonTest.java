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
 * Faithful port of the {@code unit/multi.tcl} test "EXEC and script timeout".
 *
 * <p>Unlike "MULTI and script timeout", here {@code MULTI} and the first
 * {@code INCR} are issued <em>before</em> the script starts (so they queue
 * normally); the script then becomes busy and {@code EXEC} itself arrives during
 * the timeout. Real Redis rejects {@code EXEC} during a busy script but, because
 * the rejected command is {@code EXEC}, reports it as an aborted transaction
 * that names the cause — matching {@code EXECABORT*BUSY*} rather than a bare
 * {@code BUSY}. The transaction is discarded, so the connection is no longer in
 * MULTI state afterwards.
 */
@ExtendWith(ComparisonBase.class)
public class ScriptTimeoutExecComparisonTest {

    @TestTemplate
    public void execAndScriptTimeout(Jedis jedis, HostAndPort hostAndPort) {
        jedis.configSet("lua-time-limit", "50");
        jedis.set("xx", "1");

        Jedis rd1 = new Jedis(hostAndPort.getHost(), hostAndPort.getPort(), 1_000_000);
        Jedis r2 = new Jedis(hostAndPort.getHost(), hostAndPort.getPort(), 1_000_000);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            // MULTI and the first INCR are queued BEFORE the script runs.
            r2.sendCommand(Protocol.Command.MULTI);
            r2.sendCommand(Protocol.Command.INCR, "xx");

            pool.submit(() -> {
                try {
                    rd1.eval("while true do end", 0);
                } catch (Exception ignored) {
                    // aborted by SCRIPT KILL below
                }
            });

            Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
                Thread.sleep(300); // let the script exceed lua-time-limit

                // A data command queued during the timeout is rejected with BUSY.
                ignoringErrors(() -> r2.sendCommand(Protocol.Command.INCR, "xx"));

                // EXEC arrives during the timeout -> EXECABORT naming the cause.
                String execError = null;
                try {
                    r2.sendCommand(Protocol.Command.EXEC);
                } catch (JedisDataException e) {
                    execError = e.getMessage();
                }
                assertThat(execError)
                        .as("EXEC during a busy script must abort and name the cause")
                        .contains("EXECABORT")
                        .contains("BUSY");

                jedis.scriptKill();
                Thread.sleep(300); // give the script time to actually abort

                // Nothing from the transaction ran.
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
            // mirrors the tcl `catch {...}`: BUSY is expected here
        }
    }
}
