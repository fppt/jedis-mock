package com.github.fppt.jedismock.comparisontests.notifications;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;

import java.util.Collections;

import static com.github.fppt.jedismock.comparisontests.notifications.NotificationCollector.collectorFor;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Keyspace notifications of the new-key ({@code n}) event class: a single
 * {@code new} event the first time a key comes into existence, whatever its
 * type. Unlike the other classes it is not covered by the {@code A} alias.
 */
@ExtendWith(ComparisonBase.class)
public class NewKeyKeyspaceNotificationsTest {

    @TestTemplate
    public void everyTypeOfCreationPublishesNewOnce(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "En")) {
            jedis.set("k1", "v");
            //A second write to an existing key is not a creation
            jedis.set("k1", "w");
            jedis.rpush("k2", "a");
            jedis.rpush("k2", "b");
            jedis.sadd("k3", "a");
            jedis.hset("k4", "f", "v");
            jedis.zadd("k5", 1, "a");
            jedis.xadd("k6", new StreamEntryID(1, 1), Collections.singletonMap("f", "v"));
            jedis.incr("k7");
            assertThat(events.next(7)).containsExactly(
                    "__keyevent@0__:new -> k1",
                    "__keyevent@0__:new -> k2",
                    "__keyevent@0__:new -> k3",
                    "__keyevent@0__:new -> k4",
                    "__keyevent@0__:new -> k5",
                    "__keyevent@0__:new -> k6",
                    "__keyevent@0__:new -> k7");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void newIsPublishedBeforeTheTypeEvent(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "E$n")) {
            jedis.set("k", "v");
            jedis.set("k", "w");
            assertThat(events.next(3)).containsExactly(
                    "__keyevent@0__:new -> k",
                    "__keyevent@0__:set -> k",
                    "__keyevent@0__:set -> k");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void recreatingAKeyPublishesNewAgain(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "En")) {
            jedis.set("k", "v");
            jedis.del("k");
            jedis.set("k", "v");
            assertThat(events.next(2)).containsExactly(
                    "__keyevent@0__:new -> k",
                    "__keyevent@0__:new -> k");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void overwritingAnExpiredKeyPublishesExpiredThenNew(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        //A write that finds the key already expired must first report the
        //expiry, and then treat the write as a creation
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "E$nx")) {
            jedis.psetex("k", 50, "v");
            assertThat(events.next(2)).containsExactly(
                    "__keyevent@0__:new -> k",
                    "__keyevent@0__:set -> k");
            //Deliberately do not read the key while it expires
            Thread.sleep(300);
            jedis.set("k", "again");
            assertThat(events.next(3)).containsExactly(
                    "__keyevent@0__:expired -> k",
                    "__keyevent@0__:new -> k",
                    "__keyevent@0__:set -> k");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void writingAFieldOfAnExpiredHashPublishesExpiredThenNew(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Ehnx")) {
            jedis.hset("h", "f", "v");
            jedis.pexpire("h", 50);
            assertThat(events.next(2)).containsExactly(
                    "__keyevent@0__:new -> h",
                    "__keyevent@0__:hset -> h");
            Thread.sleep(300);
            jedis.hset("h", "f", "again");
            assertThat(events.next(3)).containsExactly(
                    "__keyevent@0__:expired -> h",
                    "__keyevent@0__:new -> h",
                    "__keyevent@0__:hset -> h");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void newKeyClassIsNotPartOfTheAllAlias(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        //'A' covers every class except K, E, n and m, so 'new' stays silent
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "EA")) {
            jedis.set("k", "v");
            assertThat(events.next(1)).containsExactly("__keyevent@0__:set -> k");
            events.assertNoFurtherNotifications();
        }
    }
}
