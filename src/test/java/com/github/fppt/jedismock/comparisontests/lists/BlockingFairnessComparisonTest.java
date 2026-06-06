package com.github.fppt.jedismock.comparisontests.lists;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Reproduces the tcl test "BRPOPLPUSH with multiple blocked clients".
 *
 * Several clients block on the same source key with an infinite timeout. When a
 * value is pushed, real Redis serves the blocked clients in FIFO order: the
 * client that blocked first gets the first crack at the element. Here the first
 * client's destination is the wrong type, so it must fail with WRONGTYPE
 * (leaving the element in place), and the second client must then receive it.
 *
 * The mock used to wake every waiter via {@code notifyAll()} and let an
 * arbitrary thread win the monitor race. When the second client won, it stole
 * the only element and the first client blocked forever, hanging the test.
 * Iterating exercises the race repeatedly.
 */
@ExtendWith(ComparisonBase.class)
public class BlockingFairnessComparisonTest {

    @TestTemplate
    public void brpoplpushServesMultipleBlockedClientsInOrder(Jedis jedis, HostAndPort hostAndPort)
            throws InterruptedException {
        for (int i = 0; i < 20; i++) {
            String src = "blist_" + i;
            String dst1 = "target1_" + i;
            String dst2 = "target2_" + i;
            jedis.del(src, dst1, dst2);
            jedis.set(dst1, "nolist"); // first client's destination is the wrong type

            Jedis client1 = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
            Jedis client2 = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
            ExecutorService pool = Executors.newFixedThreadPool(2);
            AtomicReference<Throwable> rd1Error = new AtomicReference<>();
            AtomicReference<String> rd2Result = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(2);

            try {
                // Client 1 blocks first.
                pool.submit(() -> {
                    try {
                        client1.brpoplpush(src, dst1, 0);
                    } catch (Throwable t) {
                        rd1Error.set(t);
                    } finally {
                        done.countDown();
                    }
                });
                waitForBlockedClients(jedis, 1); // ensure client1 is the older waiter
                pool.submit(() -> {
                    try {
                        rd2Result.set(client2.brpoplpush(src, dst2, 0));
                    } catch (Throwable t) {
                        rd2Result.set("ERROR:" + t);
                    } finally {
                        done.countDown();
                    }
                });
                waitForBlockedClients(jedis, 2); // both registered, in order

                jedis.lpush(src, "foo");

                if (!done.await(10, TimeUnit.SECONDS)) {
                    fail("Blocked BRPOPLPUSH clients did not complete (iteration " + i
                            + ") -- a waiter is stuck forever");
                }

                assertThat(rd1Error.get())
                        .as("first client must fail with WRONGTYPE")
                        .isInstanceOf(JedisDataException.class);
                assertThat(rd1Error.get().getMessage()).startsWith("WRONGTYPE");
                assertThat(rd2Result.get())
                        .as("second client must receive the element")
                        .isEqualTo("foo");
                assertThat(jedis.lrange(dst2, 0, -1)).containsExactly("foo");
            } finally {
                pool.shutdownNow();
                client1.close();
                client2.close();
            }
        }
    }

    private static final Pattern BLOCKED_CLIENTS = Pattern.compile("blocked_clients:(\\d+)");

    /**
     * Waits until {@code INFO}'s {@code blocked_clients} reaches {@code n}, the
     * way the tcl suite's {@code wait_for_blocked_clients_count} does. This
     * makes blocking-order deterministic for both real Redis and the mock.
     */
    private void waitForBlockedClients(Jedis jedis, int n) {
        Awaitility.await().until(() -> blockedClients(jedis) == n);
    }

    private int blockedClients(Jedis jedis) {
        Matcher matcher = BLOCKED_CLIENTS.matcher(jedis.info("clients"));
        return matcher.find() ? Integer.parseInt(matcher.group(1)) : 0;
    }
}
