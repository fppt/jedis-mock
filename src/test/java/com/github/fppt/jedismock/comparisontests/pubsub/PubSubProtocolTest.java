package com.github.fppt.jedismock.comparisontests.pubsub;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Protocol;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Wire-level subscribe/unsubscribe semantics of real Redis:
 * <ul>
 * <li>every SUBSCRIBE/UNSUBSCRIBE argument gets its own acknowledgement
 * (a separate three-element array), even duplicates and channels the
 * client was never subscribed to;</li>
 * <li>the count in each acknowledgement is the total number of channel
 * <em>and</em> pattern subscriptions the client holds;</li>
 * <li>UNSUBSCRIBE/PUNSUBSCRIBE without arguments replies once with a nil
 * channel when there are no subscriptions at all;</li>
 * <li>PING inside subscribe mode gets the two-element array reply;</li>
 * <li>PUBSUB NUMSUB reports per-channel subscriber counts.</li>
 * </ul>
 */
@ExtendWith(ComparisonBase.class)
public class PubSubProtocolTest {

    static class RecordingSubscriber extends JedisPubSub {
        final List<Map.Entry<String, Integer>> subscribeEvents = new CopyOnWriteArrayList<>();
        final List<Map.Entry<String, Integer>> psubscribeEvents = new CopyOnWriteArrayList<>();
        final List<Map.Entry<String, Integer>> unsubscribeEvents = new CopyOnWriteArrayList<>();

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            subscribeEvents.add(new AbstractMap.SimpleEntry<>(channel, subscribedChannels));
        }

        @Override
        public void onPSubscribe(String pattern, int subscribedChannels) {
            psubscribeEvents.add(new AbstractMap.SimpleEntry<>(pattern, subscribedChannels));
        }

        @Override
        public void onUnsubscribe(String channel, int subscribedChannels) {
            unsubscribeEvents.add(new AbstractMap.SimpleEntry<>(channel, subscribedChannels));
        }
    }

    static class TestSubscription implements AutoCloseable {
        private final Jedis client;
        private final RecordingSubscriber subscriber = new RecordingSubscriber();
        private final ExecutorService service = Executors.newSingleThreadExecutor();
        private final Future<?> future;

        TestSubscription(HostAndPort hostAndPort, String... channels) {
            client = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
            future = service.submit(() -> client.subscribe(subscriber, channels));
        }

        RecordingSubscriber subscriber() {
            return subscriber;
        }

        @Override
        public void close() throws Exception {
            try {
                if (subscriber.isSubscribed()) {
                    subscriber.unsubscribe();
                    subscriber.punsubscribe();
                }
                //The timeout protects the suite from hanging when the server
                //(incorrectly) does not acknowledge the unsubscribe.
                future.get(5, TimeUnit.SECONDS);
            } catch (TimeoutException | java.util.concurrent.ExecutionException e) {
                //Fall through to the forced disconnect below.
            } finally {
                service.shutdownNow();
                client.disconnect();
                client.close();
            }
        }
    }

    @TestTemplate
    public void subscribeIsAcknowledgedPerChannelWithRunningCount(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (TestSubscription subscription = new TestSubscription(hostAndPort, "proto_foo", "proto_bar")) {
            RecordingSubscriber subscriber = subscription.subscriber();
            Awaitility.await().until(() -> subscriber.subscribeEvents.size() == 2);
            assertThat(subscriber.subscribeEvents).containsExactly(
                    new AbstractMap.SimpleEntry<>("proto_foo", 1),
                    new AbstractMap.SimpleEntry<>("proto_bar", 2));
        }
    }

    @TestTemplate
    public void duplicateSubscribeIsAcknowledgedPerArgument(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (TestSubscription subscription = new TestSubscription(hostAndPort, "proto_dup", "proto_dup")) {
            RecordingSubscriber subscriber = subscription.subscriber();
            //A duplicate channel does not create a second subscription,
            //but it is acknowledged all the same.
            Awaitility.await().until(() -> subscriber.subscribeEvents.size() == 2);
            assertThat(subscriber.subscribeEvents).containsExactly(
                    new AbstractMap.SimpleEntry<>("proto_dup", 1),
                    new AbstractMap.SimpleEntry<>("proto_dup", 1));
        }
    }

    @TestTemplate
    public void subscriptionCountCombinesChannelsAndPatterns(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (TestSubscription subscription = new TestSubscription(hostAndPort, "proto_mix")) {
            RecordingSubscriber subscriber = subscription.subscriber();
            Awaitility.await().until(() -> subscriber.subscribeEvents.size() == 1);
            subscriber.psubscribe("proto_mix.*");
            Awaitility.await().until(() -> subscriber.psubscribeEvents.size() == 1);
            //One channel plus one pattern: the pattern acknowledgement counts both.
            assertThat(subscriber.psubscribeEvents).containsExactly(
                    new AbstractMap.SimpleEntry<>("proto_mix.*", 2));
        }
    }

    @TestTemplate
    public void unsubscribeFromNonSubscribedChannelIsAcknowledged(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (TestSubscription subscription = new TestSubscription(hostAndPort, "proto_keep")) {
            RecordingSubscriber subscriber = subscription.subscriber();
            Awaitility.await().until(() -> subscriber.subscribeEvents.size() == 1);
            subscriber.unsubscribe("proto_ghost");
            Awaitility.await().until(() -> subscriber.unsubscribeEvents.size() == 1);
            assertThat(subscriber.unsubscribeEvents).containsExactly(
                    new AbstractMap.SimpleEntry<>("proto_ghost", 1));
        }
    }

    @TestTemplate
    public void pingInSubscribeModeGetsArrayReply(Jedis jedis, HostAndPort hostAndPort) {
        //Jedis itself accepts a plain +PONG here, so assert the raw reply
        //shape: in RESP2 subscribe mode PING is answered with the array
        //form [pong, message-or-empty].
        try (Jedis client = new Jedis(hostAndPort.getHost(), hostAndPort.getPort(), 2000)) {
            client.sendCommand(Protocol.Command.SUBSCRIBE, "proto_ping");
            Object noArgReply = client.sendCommand(Protocol.Command.PING);
            assertThat(noArgReply).isInstanceOf(List.class);
            List<?> pong = (List<?>) noArgReply;
            assertThat(pong).hasSize(2);
            assertThat(new String((byte[]) pong.get(0))).isEqualTo("pong");
            assertThat(new String((byte[]) pong.get(1))).isEmpty();

            Object messageReply = client.sendCommand(Protocol.Command.PING, "foo");
            assertThat(messageReply).isInstanceOf(List.class);
            List<?> pongFoo = (List<?>) messageReply;
            assertThat(pongFoo).hasSize(2);
            assertThat(new String((byte[]) pongFoo.get(0))).isEqualTo("pong");
            assertThat(new String((byte[]) pongFoo.get(1))).isEqualTo("foo");
        }
    }

    @TestTemplate
    public void pubsubNumSubReportsPerChannelCounts(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (TestSubscription subscription = new TestSubscription(hostAndPort, "proto_counted")) {
            RecordingSubscriber subscriber = subscription.subscriber();
            Awaitility.await().until(() -> subscriber.subscribeEvents.size() == 1);
            Map<String, Long> counts = jedis.pubsubNumSub("proto_counted", "proto_missing");
            assertThat(counts).containsOnly(
                    new AbstractMap.SimpleEntry<>("proto_counted", 1L),
                    new AbstractMap.SimpleEntry<>("proto_missing", 0L));
        }
    }

    @TestTemplate
    public void subscriptionsAreDroppedOnDisconnect(Jedis jedis, HostAndPort hostAndPort) {
        Jedis client = new Jedis(hostAndPort.getHost(), hostAndPort.getPort(), 2000);
        try {
            client.sendCommand(Protocol.Command.SUBSCRIBE, "proto_dropped");
            client.sendCommand(Protocol.Command.PSUBSCRIBE, "proto_dropped.*");
            Awaitility.await().until(() ->
                    jedis.pubsubChannels("proto_dropped*").contains("proto_dropped"));
        } finally {
            client.disconnect();
            client.close();
        }
        //A dead client must not linger in the pub/sub registries.
        Awaitility.await().until(() -> jedis.pubsubChannels("proto_dropped*").isEmpty());
        Awaitility.await().until(() -> jedis.pubsubNumPat() == 0);
    }

    @TestTemplate
    public void emptyUnsubscribeWithoutSubscriptionsIsAcknowledged(Jedis jedis, HostAndPort hostAndPort) {
        try (Jedis client = new Jedis(hostAndPort.getHost(), hostAndPort.getPort(), 2000)) {
            List<Object> replies = new ArrayList<>();
            replies.add(client.sendCommand(Protocol.Command.UNSUBSCRIBE));
            replies.add(client.sendCommand(Protocol.Command.PUNSUBSCRIBE));
            List<String> expected = Arrays.asList("unsubscribe", "punsubscribe");
            for (int i = 0; i < replies.size(); i++) {
                assertThat(replies.get(i)).as(expected.get(i)).isInstanceOf(List.class);
                List<?> reply = (List<?>) replies.get(i);
                assertThat(reply).hasSize(3);
                assertThat(new String((byte[]) reply.get(0))).isEqualTo(expected.get(i));
                assertThat(reply.get(1)).isNull();
                assertThat(reply.get(2)).isEqualTo(0L);
            }
        }
    }
}
