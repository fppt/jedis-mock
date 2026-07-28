package com.github.fppt.jedismock.comparisontests.pubsub;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import com.github.fppt.jedismock.util.MockPSubscriber;
import com.github.fppt.jedismock.util.MockSubscriber;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pub/Sub in Redis is server-wide: it is completely independent of the
 * key space, so a message published on database 10 is received by a
 * subscriber on database 1, PUBSUB introspection sees subscribers
 * regardless of their selected database, and FLUSHDB/FLUSHALL (which
 * only clear the key space) do not affect subscriptions.
 */
@ExtendWith(ComparisonBase.class)
public class ServerWidePubSubTest {

    static class AckAwareSubscriber extends MockSubscriber {
        private final CountDownLatch subscribed = new CountDownLatch(1);

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            subscribed.countDown();
        }

        void awaitSubscription() throws InterruptedException {
            assertThat(subscribed.await(10, TimeUnit.SECONDS))
                    .as("subscription acknowledged")
                    .isTrue();
        }
    }

    static class AckAwarePSubscriber extends MockPSubscriber {
        private final CountDownLatch subscribed = new CountDownLatch(1);

        @Override
        public void onPSubscribe(String pattern, int subscribedChannels) {
            subscribed.countDown();
        }

        void awaitSubscription() throws InterruptedException {
            assertThat(subscribed.await(10, TimeUnit.SECONDS))
                    .as("pattern subscription acknowledged")
                    .isTrue();
        }
    }

    static class TestSubscriptionOnDb implements AutoCloseable {
        private final Jedis client;
        private final AckAwareSubscriber subscriber = new AckAwareSubscriber();
        private final ExecutorService service = Executors.newSingleThreadExecutor();
        private final Future<?> future;

        TestSubscriptionOnDb(HostAndPort hostAndPort, int db, String... channels) {
            client = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
            client.select(db);
            future = service.submit(() -> client.subscribe(subscriber, channels));
        }

        AckAwareSubscriber subscriber() {
            return subscriber;
        }

        @Override
        public void close() throws Exception {
            try {
                subscriber.unsubscribe();
                //The timeout protects the suite from hanging when the server
                //has (incorrectly) forgotten the subscription and therefore
                //never acknowledges the unsubscribe.
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

    static class TestPSubscriptionOnDb implements AutoCloseable {
        private final Jedis client;
        private final AckAwarePSubscriber subscriber = new AckAwarePSubscriber();
        private final ExecutorService service = Executors.newSingleThreadExecutor();
        private final Future<?> future;

        TestPSubscriptionOnDb(HostAndPort hostAndPort, int db, String... patterns) {
            client = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
            client.select(db);
            future = service.submit(() -> client.psubscribe(subscriber, patterns));
        }

        AckAwarePSubscriber subscriber() {
            return subscriber;
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

    @TestTemplate
    public void channelMessageIsDeliveredAcrossDatabases(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (TestSubscriptionOnDb subscription =
                     new TestSubscriptionOnDb(hostAndPort, 1, "cross_db_channel")) {
            subscription.subscriber().awaitSubscription();
            //The publisher is on database 0, the subscriber on database 1.
            assertThat(jedis.publish("cross_db_channel", "hello")).isEqualTo(1);
            assertThat(subscription.subscriber().latestChannel()).isEqualTo("cross_db_channel");
            assertThat(subscription.subscriber().latestMessage()).isEqualTo("hello");
        }
    }

    @TestTemplate
    public void patternMessageIsDeliveredAcrossDatabases(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (TestPSubscriptionOnDb subscription =
                     new TestPSubscriptionOnDb(hostAndPort, 1, "cross_db_*")) {
            subscription.subscriber().awaitSubscription();
            assertThat(jedis.publish("cross_db_channel", "hello")).isEqualTo(1);
            assertThat(subscription.subscriber().latestPattern()).isEqualTo("cross_db_*");
            assertThat(subscription.subscriber().latestChannel()).isEqualTo("cross_db_channel");
            assertThat(subscription.subscriber().latestMessage()).isEqualTo("hello");
        }
    }

    @TestTemplate
    public void introspectionSeesSubscribersOnOtherDatabases(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (TestSubscriptionOnDb subscription =
                     new TestSubscriptionOnDb(hostAndPort, 1, "cross_db_channel");
             TestPSubscriptionOnDb pSubscription =
                     new TestPSubscriptionOnDb(hostAndPort, 1, "cross_db_*")) {
            subscription.subscriber().awaitSubscription();
            pSubscription.subscriber().awaitSubscription();
            assertThat(jedis.pubsubChannels("*")).contains("cross_db_channel");
            assertThat(jedis.pubsubNumPat()).isEqualTo(1);
        }
    }

    @TestTemplate
    public void flushDBDoesNotDropSubscriptions(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (TestSubscriptionOnDb subscription =
                     new TestSubscriptionOnDb(hostAndPort, 0, "durable_channel");
             TestPSubscriptionOnDb pSubscription =
                     new TestPSubscriptionOnDb(hostAndPort, 0, "durable_*")) {
            subscription.subscriber().awaitSubscription();
            pSubscription.subscriber().awaitSubscription();
            jedis.flushDB();
            assertThat(jedis.publish("durable_channel", "hello")).isEqualTo(2);
            assertThat(subscription.subscriber().latestMessage()).isEqualTo("hello");
            assertThat(pSubscription.subscriber().latestMessage()).isEqualTo("hello");
        }
    }

    @TestTemplate
    public void flushAllDoesNotDropSubscriptions(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (TestSubscriptionOnDb subscription =
                     new TestSubscriptionOnDb(hostAndPort, 0, "durable_channel");
             TestPSubscriptionOnDb pSubscription =
                     new TestPSubscriptionOnDb(hostAndPort, 0, "durable_*")) {
            subscription.subscriber().awaitSubscription();
            pSubscription.subscriber().awaitSubscription();
            jedis.flushAll();
            assertThat(jedis.publish("durable_channel", "hello")).isEqualTo(2);
            assertThat(subscription.subscriber().latestMessage()).isEqualTo("hello");
            assertThat(pSubscription.subscriber().latestMessage()).isEqualTo("hello");
        }
    }
}
