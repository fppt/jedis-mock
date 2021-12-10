package com.github.fppt.jedismock.comparisontests.pubsub;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import com.github.fppt.jedismock.util.MockSubscriber;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(ComparisonBase.class)
public class PubSubTest {

    class TestSubscription implements AutoCloseable {
        private final Jedis client;
        private final MockSubscriber subscriber;
        private final Future<?> future;

        TestSubscription(String host, int port, String... channels) {
            client = new Jedis(host, port);
            subscriber = new MockSubscriber();
            ExecutorService service = Executors.newSingleThreadExecutor();
            future = service.submit(() -> client.subscribe(subscriber, channels));
        }

        public MockSubscriber getSubscriber() {
            return subscriber;
        }

        @Override
        public void close() throws Exception {
            subscriber.unsubscribe();
            future.get();
            client.quit();
        }
    }

    @TestTemplate
    void pubSubChannelsReturnsChannels(Jedis jedis) throws Exception {
        Client client = jedis.getClient();
        try (TestSubscription foo = new TestSubscription(client.getHost(), client.getPort(), "foo");
             TestSubscription bar = new TestSubscription(client.getHost(), client.getPort(), "bar")) {
            Set<String> expected = new HashSet<>(Arrays.asList("foo", "bar"));
            Awaitility.await().until(
                    () -> expected.equals(new HashSet<>(jedis.pubsubChannels("*"))));
        }
    }

    @TestTemplate
    void channelsWithNoSubscriptionDisappear(Jedis jedis) throws Exception {
        Client client = jedis.getClient();
        try (TestSubscription ignore =
                     new TestSubscription(client.getHost(), client.getPort(), "foo")) {
            Awaitility.await().until(
                    () -> Collections.singleton("foo").equals(new HashSet<>(jedis.pubsubChannels("*"))));
        }
        Awaitility.await().until(
                () -> jedis.pubsubChannels("*").isEmpty());
    }


    @TestTemplate
    void pubSubChannelsRespectsPattern(Jedis jedis) throws Exception {
        Client client = jedis.getClient();
        try (TestSubscription foo = new TestSubscription(client.getHost(), client.getPort(), "foo");
             TestSubscription bar = new TestSubscription(client.getHost(), client.getPort(), "bar")) {
            Awaitility.await().until(
                    () -> Collections.singleton("bar").equals(new HashSet<>(jedis.pubsubChannels("b*"))));
        }
    }

    @TestTemplate
    public void whenSubscribingToAChannelPublishedMessagesAreReceived(Jedis jedis) throws Exception {
        String channel = "normalbob";
        String message = "SUPERBOB";

        Client client = jedis.getClient();
        try (TestSubscription subscription = new TestSubscription(client.getHost(), client.getPort(), channel)) {
            Awaitility.await().until(() -> jedis.pubsubChannels("*").contains(channel));
            jedis.publish(channel, message);
            assertEquals(channel, subscription.getSubscriber().latestChannel());
            assertEquals(message, subscription.getSubscriber().latestMessage());
        }
    }

    @TestTemplate
    public void whenSubscribingToMultipleChannelsPublishedMessagesAreReceived(Jedis jedis) throws Exception {
        String message = "SUPERBOB";

        Client client = jedis.getClient();
        try (TestSubscription s1 = new TestSubscription(client.getHost(), client.getPort(), "foo", "bar");
             TestSubscription s2 = new TestSubscription(client.getHost(), client.getPort(), "bar")) {
            Awaitility.await().until(() -> jedis.pubsubChannels("*").contains("bar"));
            assertEquals(2, jedis.publish("bar", message));
            assertEquals("bar", s1.getSubscriber().latestChannel());
            assertEquals(message, s1.getSubscriber().latestMessage());
            assertEquals("bar", s2.getSubscriber().latestChannel());
            assertEquals(message, s2.getSubscriber().latestMessage());
        }
    }
}
