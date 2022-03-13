package com.github.fppt.jedismock.comparisontests.pubsub;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import com.github.fppt.jedismock.util.MockPSubscriber;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(ComparisonBase.class)
public class PSubscribeTest {

    static class TestPSubscription implements AutoCloseable {
        private final Jedis client;
        private final MockPSubscriber subscriber;
        private final Future<?> future;

        public TestPSubscription(String host, int port, String... patterns) {
            client = new Jedis(host, port);
            subscriber = new MockPSubscriber();
            ExecutorService service = Executors.newSingleThreadExecutor();
            future = service.submit(() -> client.psubscribe(subscriber, patterns));
        }

        public MockPSubscriber getSubscriber() {
            return subscriber;
        }

        @Override
        public void close() throws Exception {
            subscriber.punsubscribe();
            future.get();
            client.quit();
        }
    }

    @TestTemplate
    public void whenSubscribingToAChannelPublishedMessagesAreReceived(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        String pattern = "n[eo]rm*l?ob";
        String channel = "normaaalbob";
        String message = "SUPERBOB";

        try (TestPSubscription subscription = new TestPSubscription(hostAndPort.getHost(), hostAndPort.getPort(), pattern)) {
            Awaitility.await().until(() -> jedis.pubsubNumPat() > 0);
            jedis.publish(channel, message);
            assertEquals(pattern, subscription.getSubscriber().latestPattern());
            assertEquals(channel, subscription.getSubscriber().latestChannel());
            assertEquals(message, subscription.getSubscriber().latestMessage());
        }
    }
}
