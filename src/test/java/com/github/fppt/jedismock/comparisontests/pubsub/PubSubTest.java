package com.github.fppt.jedismock.comparisontests.pubsub;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import com.github.fppt.jedismock.util.MockSubscriber;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class PubSubTest {

    static class TestSubscription implements AutoCloseable {
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
            client.disconnect();
        }
    }

    @TestTemplate
    void pubSubChannelsReturnsChannels(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (TestSubscription foo = new TestSubscription(hostAndPort.getHost(), hostAndPort.getPort(), "foo");
             TestSubscription bar = new TestSubscription(hostAndPort.getHost(), hostAndPort.getPort(), "bar")) {
            Set<String> expected = new HashSet<>(Arrays.asList("foo", "bar"));
            Awaitility.await().until(
                    () -> expected.equals(new HashSet<>(jedis.pubsubChannels("*"))));
        }
    }

    @TestTemplate
    void channelsWithNoSubscriptionDisappear(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (TestSubscription ignore =
                     new TestSubscription(hostAndPort.getHost(), hostAndPort.getPort(), "foo")) {
            Awaitility.await().until(
                    () -> Collections.singleton("foo").equals(new HashSet<>(jedis.pubsubChannels("*"))));
        }
        Awaitility.await().until(
                () -> jedis.pubsubChannels("*").isEmpty());
    }


    @TestTemplate
    void pubSubChannelsRespectsPattern(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (TestSubscription foo = new TestSubscription(hostAndPort.getHost(), hostAndPort.getPort(), "foo");
             TestSubscription bar = new TestSubscription(hostAndPort.getHost(), hostAndPort.getPort(), "bar")) {
            Awaitility.await().until(
                    () -> Collections.singleton("bar").equals(new HashSet<>(jedis.pubsubChannels("b*"))));
        }
    }

    @TestTemplate
    public void whenSubscribingToAChannelPublishedMessagesAreReceived(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        String channel = "normalbob";
        String message = "SUPERBOB";

        try (TestSubscription subscription = new TestSubscription(hostAndPort.getHost(), hostAndPort.getPort(), channel)) {
            Awaitility.await().until(() -> jedis.pubsubChannels("*").contains(channel));
            jedis.publish(channel, message);
            assertThat(subscription.getSubscriber().latestChannel()).isEqualTo(channel);
            assertThat(subscription.getSubscriber().latestMessage()).isEqualTo(message);
            //Verify that the message is received only once
            Awaitility.await().during(Duration.ofSeconds(1)).until(() -> subscription.getSubscriber().getMsgCount() == 1);
        }
    }

    @TestTemplate
    public void whenSubscribingToMultipleChannelsPublishedMessagesAreReceived(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        String message = "SUPERBOB";

        try (TestSubscription s1 = new TestSubscription(hostAndPort.getHost(), hostAndPort.getPort(), "foo", "bar");
             TestSubscription s2 = new TestSubscription(hostAndPort.getHost(), hostAndPort.getPort(), "bar")) {
            Awaitility.await().until(() -> jedis.pubsubChannels("*").contains("bar"));
            assertThat(jedis.publish("bar", message)).isEqualTo(2);
            assertThat(s1.getSubscriber().latestChannel()).isEqualTo("bar");
            assertThat(s1.getSubscriber().latestMessage()).isEqualTo(message);
            assertThat(s2.getSubscriber().latestChannel()).isEqualTo("bar");
            assertThat(s2.getSubscriber().latestMessage()).isEqualTo(message);
        }
    }
}
