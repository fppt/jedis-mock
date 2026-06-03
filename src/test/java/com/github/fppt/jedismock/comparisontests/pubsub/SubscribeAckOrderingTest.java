package com.github.fppt.jedismock.comparisontests.pubsub;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comparison test highlighting the pub/sub ordering defect behind
 * https://github.com/fppt/jedis-mock/issues/375
 * (originally https://github.com/redisson/redisson/issues/6700).
 *
 * <p>Redis guarantees that, on a single connection, the {@code subscribe}
 * acknowledgement is delivered <b>before</b> any {@code message} for that channel.
 * Redisson relies on this: it creates the channel's {@code PubSubEntry} when the
 * {@code subscribe} ack arrives and looks it up for every subsequent message. If a
 * message overtakes the ack, the lookup returns {@code null} and Redisson's
 * {@code CommandPubSubDecoder} fails ("entry is null" NPE on 3.50.0, "Unable to
 * decode data" on newer versions).
 *
 * <p>JedisMock breaks this guarantee. {@code Subscribe} registers the subscriber
 * inside the global lock, but the {@code subscribe} ack is written to the socket
 * <i>outside</i> the lock (back in {@code RedisClient#run}). A concurrent
 * {@code PUBLISH} runs inside the lock and writes the message directly into the
 * just-registered subscriber's socket, which can overtake the not-yet-sent ack.
 *
 * <p>{@link JedisPubSub} dispatches its callbacks on a single thread in the exact
 * order frames arrive on the wire, so the order in which {@code onSubscribe} and
 * {@code onMessage} fire is precisely the on-the-wire ordering we care about. This
 * {@link TestTemplate} runs against both servers:
 * <ul>
 *   <li><b>real</b>: passes -- {@code onSubscribe} always fires first.</li>
 *   <li><b>mock</b>: fails -- {@code onMessage} fires before {@code onSubscribe}.</li>
 * </ul>
 */
@ExtendWith(ComparisonBase.class)
public class SubscribeAckOrderingTest {

    private static final String PAYLOAD = "unlock";
    private static final int ROUNDS = 3000;

    @TestTemplate
    public void subscribeAckIsAlwaysDeliveredBeforeAnyMessage(HostAndPort hostAndPort) throws Exception {
        final AtomicReference<String> currentChannel = new AtomicReference<>("warmup");
        final AtomicBoolean stopPublisher = new AtomicBoolean(false);

        // A publisher that continuously floods PUBLISH at the channel currently under
        // test. The instant a subscriber registers itself, the next flooded PUBLISH is
        // delivered into its socket -- racing the subscribe ack that has not been sent yet.
        Jedis publisher = new Jedis(hostAndPort);
        Thread pubThread = new Thread(() -> {
            try {
                while (!stopPublisher.get()) {
                    publisher.publish(currentChannel.get(), PAYLOAD);
                }
            } catch (RuntimeException ignored) {
                // connection closed during teardown
            }
        }, "publisher-flood");
        pubThread.setDaemon(true);
        pubThread.start();

        ExecutorService subscribeExecutor = Executors.newCachedThreadPool();
        String violatingChannel = null;
        String firstCallback = null;
        try {
            for (int r = 0; r < ROUNDS; r++) {
                String channel = "ack-order-channel-" + r;
                currentChannel.set(channel);

                // Fresh subscriber per round: we only care which callback fires first.
                Jedis subscriber = new Jedis(hostAndPort);
                AtomicReference<String> first = new AtomicReference<>();
                CountDownLatch firstCallbackFired = new CountDownLatch(1);

                JedisPubSub pubSub = new JedisPubSub() {
                    @Override
                    public void onSubscribe(String ch, int subscribedChannels) {
                        first.compareAndSet(null, "onSubscribe");
                        firstCallbackFired.countDown();
                        unsubscribe(); // end the (otherwise endless) subscription
                    }

                    @Override
                    public void onMessage(String ch, String message) {
                        first.compareAndSet(null, "onMessage");
                        firstCallbackFired.countDown();
                    }
                };

                // jedis.subscribe(...) blocks until unsubscribed, so run it off-thread.
                Future<?> subscription = subscribeExecutor.submit(() -> {
                    try {
                        subscriber.subscribe(pubSub, channel);
                    } catch (RuntimeException ignored) {
                        // subscriber.close() below unblocks this by breaking the socket
                    }
                });

                firstCallbackFired.await(5, TimeUnit.SECONDS);
                // Closing the connection unblocks subscribe() regardless of how far it got.
                subscriber.close();
                subscription.get(5, TimeUnit.SECONDS);

                if (!"onSubscribe".equals(first.get())) {
                    violatingChannel = channel;
                    firstCallback = first.get();
                    break;
                }
            }
        } finally {
            stopPublisher.set(true);
            pubThread.join(2000);
            publisher.close();
            subscribeExecutor.shutdownNow();
        }

        assertThat(violatingChannel)
                .as("Redis guarantees the subscribe ack precedes any message on a connection, so "
                        + "onSubscribe must fire before onMessage. But on channel [%s] the first "
                        + "JedisPubSub callback was [%s]: JedisMock delivered a published message "
                        + "before the subscribe ack -- the condition that crashes Redisson's "
                        + "CommandPubSubDecoder (issue #375).", violatingChannel, firstCallback)
                .isNull();
    }
}
