package com.github.fppt.jedismock.comparisontests.notifications;

import org.testcontainers.shaded.org.awaitility.Awaitility;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Collects keyspace notifications in arrival order for the notification
 * comparison tests. Subscribes to every {@code __keyspace@*__}/
 * {@code __keyevent@*__} channel and records each message as
 * {@code "<channel> -> <payload>"}, so a test can assert an exact sequence.
 */
public final class NotificationCollector implements AutoCloseable {

    private static final String PARAM = "notify-keyspace-events";

    private final Jedis client;
    private final BlockingQueue<String> events = new LinkedBlockingQueue<>();
    private final JedisPubSub subscriber;
    private final ExecutorService service = Executors.newSingleThreadExecutor();
    private final Future<?> future;

    private NotificationCollector(HostAndPort hostAndPort) {
        client = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
        subscriber = new JedisPubSub() {
            @Override
            public void onPMessage(String pattern, String channel, String message) {
                events.add(channel + " -> " + message);
            }
        };
        future = service.submit(() -> client.psubscribe(subscriber, "__key*@*__:*"));
    }

    /**
     * Clears the key space, enables the given event flags and starts
     * collecting, returning only once the subscription is established.
     */
    public static NotificationCollector collectorFor(Jedis jedis, HostAndPort hostAndPort, String flags) {
        jedis.flushAll();
        jedis.configSet(PARAM, flags);
        NotificationCollector collector = new NotificationCollector(hostAndPort);
        Awaitility.await().until(() -> jedis.pubsubNumPat() > 0);
        return collector;
    }

    /** Waits for exactly {@code count} notifications and returns them in arrival order. */
    public List<String> next(int count) throws InterruptedException {
        List<String> received = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String event = events.poll(10, TimeUnit.SECONDS);
            assertThat(event).as("notification %d of %d (got %s)", i + 1, count, received).isNotNull();
            received.add(event);
        }
        return received;
    }

    public void assertNoFurtherNotifications() throws InterruptedException {
        assertThat(events.poll(300, TimeUnit.MILLISECONDS)).isNull();
    }

    @Override
    public void close() throws Exception {
        try {
            subscriber.punsubscribe();
            future.get(5, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            //Fall through to the forced disconnect below.
        } finally {
            service.shutdownNow();
            client.disconnect();
            client.close();
        }
    }
}
