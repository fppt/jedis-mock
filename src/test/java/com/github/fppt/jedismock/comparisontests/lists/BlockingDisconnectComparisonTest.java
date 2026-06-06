package com.github.fppt.jedismock.comparisontests.lists;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comparison tests for issue #317: a blocking command must be cancelled when
 * its client disconnects while still blocked.
 *
 * Real Redis frees a disconnecting client and removes it from the per-key
 * blocked-client lists, so data pushed after the disconnect is NOT consumed by
 * the gone client. The mock instead leaves the server-side thread parked in
 * {@code lock.wait()} (nothing interrupts it on close, and the per-connection
 * read loop is stuck inside the blocking command, so it cannot notice the dead
 * socket). A later write wakes the orphan, which mutates the store and then
 * fails to deliver the result down the dead socket.
 *
 * Each test asserts the correct (real-Redis) behaviour, so the "real" variant
 * passes and the "mock" variant fails -- that contrast is the point of the
 * investigation.
 */
@ExtendWith(ComparisonBase.class)
public class BlockingDisconnectComparisonTest {

    /**
     * Drives one client to block on {@code command}, lets the server register
     * the wait, then closes that client's socket (sending FIN/RST) to simulate
     * a disconnect while still blocked, and waits for the server to process the
     * disconnect. Closing the socket -- not {@code shutdownNow()} -- is what
     * actually unblocks a thread parked in a blocking socket read, so the close
     * happens BEFORE the post-disconnect wait. By the time this returns, a
     * correct server has had time to cancel the block; any later write must
     * therefore not be delivered to the gone client.
     */
    private void blockThenDisconnect(Jedis blockedClient, Runnable blockingCommand)
            throws InterruptedException {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            pool.submit(() -> {
                try {
                    blockingCommand.run();
                } catch (Exception ignored) {
                    // The socket is closed under the blocking read -- expected.
                }
            });
            Thread.sleep(500);            // let the server register the blocking wait
            blockedClient.close();        // disconnect while still blocked
            pool.shutdownNow();
            pool.awaitTermination(2, TimeUnit.SECONDS);
            Thread.sleep(700);            // let the server process the disconnect & cancel the block
        } finally {
            pool.shutdownNow();
        }
    }

    @TestTemplate
    public void blpopIsCancelledWhenClientDisconnects(Jedis jedis, HostAndPort hostAndPort)
            throws InterruptedException {
        String key = "disconnect_blpop_key";
        jedis.del(key);

        Jedis blockedClient = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
        blockThenDisconnect(blockedClient, () -> blockedClient.blpop(0, key));

        // The disconnected client must not consume this value.
        jedis.rpush(key, "foo");
        Thread.sleep(300); // give any orphan a chance to (wrongly) steal it

        assertThat(jedis.lrange(key, 0, -1)).containsExactly("foo");
    }

    @TestTemplate
    public void brpoplpushIsCancelledWhenClientDisconnects(Jedis jedis, HostAndPort hostAndPort)
            throws InterruptedException {
        String src = "disconnect_brpoplpush_src";
        String dst = "disconnect_brpoplpush_dst";
        jedis.del(src, dst);

        Jedis blockedClient = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
        blockThenDisconnect(blockedClient, () -> blockedClient.brpoplpush(src, dst, 0));

        // The disconnected client must not move this value to dst.
        jedis.rpush(src, "foo");
        Thread.sleep(300);

        assertThat(jedis.lrange(src, 0, -1)).containsExactly("foo");
        assertThat(jedis.lrange(dst, 0, -1)).isEmpty();
    }

    /**
     * The flakiness mechanism end-to-end: one "test" leaves a blocked client
     * dangling on a key, a later "test" reuses the same key. On real Redis the
     * first client was cancelled, so the second push behaves normally. On the
     * mock, the orphan from the first phase steals the second phase's value.
     */
    @TestTemplate
    public void orphanedBlockDoesNotLeakIntoReusedKey(Jedis jedis, HostAndPort hostAndPort)
            throws InterruptedException {
        String key = "reused_blocking_key";
        jedis.del(key);

        // Phase 1: a deferring client blocks and then goes away.
        Jedis phase1Client = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
        blockThenDisconnect(phase1Client, () -> phase1Client.blpop(0, key));

        // Phase 2: an unrelated, live client uses the same key normally.
        jedis.rpush(key, "a", "b", "c");
        Thread.sleep(300);

        assertThat(jedis.lrange(key, 0, -1)).containsExactly("a", "b", "c");
    }
}
