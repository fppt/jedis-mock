package com.github.fppt.jedismock.comparisontests.notifications;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;

import java.util.Collections;

import static com.github.fppt.jedismock.comparisontests.notifications.NotificationCollector.collectorFor;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Keyspace notifications of the stream ({@code t}) event class, for the
 * commands the mock implements. The consumer-group commands ({@code XGROUP},
 * {@code XREADGROUP}, {@code XCLAIM}, {@code XSETID}) are not supported at
 * all, so their events — {@code xgroup-create} and friends — are out of scope.
 */
@ExtendWith(ComparisonBase.class)
public class StreamKeyspaceNotificationsTest {

    @TestTemplate
    public void streamWritesPublishTheirEvents(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Et")) {
            jedis.xadd("st", new StreamEntryID(1, 1), Collections.singletonMap("f", "v"));
            jedis.xadd("st", new StreamEntryID(2, 2), Collections.singletonMap("f", "v"));
            jedis.xdel("st", new StreamEntryID(1, 1));
            jedis.xtrim("st", 0, false);
            assertThat(events.next(4)).containsExactly(
                    "__keyevent@0__:xadd -> st",
                    "__keyevent@0__:xadd -> st",
                    "__keyevent@0__:xdel -> st",
                    "__keyevent@0__:xtrim -> st");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void bothChannelFamiliesCarryStreamEvents(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "KEt")) {
            jedis.xadd("st", new StreamEntryID(1, 1), Collections.singletonMap("f", "v"));
            assertThat(events.next(2)).containsExactly(
                    "__keyspace@0__:st -> xadd",
                    "__keyevent@0__:xadd -> st");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void addWithTrimmingPublishesAddThenTrim(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Et")) {
            jedis.xadd("st", new StreamEntryID(1, 1), Collections.singletonMap("f", "v"));
            //An XADD that also trims reports both events
            jedis.xadd("st", XAddParams.xAddParams().id(new StreamEntryID(2, 2)).maxLen(1),
                    Collections.singletonMap("f", "v"));
            assertThat(events.next(3)).containsExactly(
                    "__keyevent@0__:xadd -> st",
                    "__keyevent@0__:xadd -> st",
                    "__keyevent@0__:xtrim -> st");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void commandsThatChangeNothingAreSilent(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Et")) {
            jedis.xadd("st", new StreamEntryID(1, 1), Collections.singletonMap("f", "v"));
            assertThat(events.next(1)).containsExactly("__keyevent@0__:xadd -> st");
            //Deleting an absent id and a no-op trim change nothing
            jedis.xdel("st", new StreamEntryID(9, 9));
            jedis.xtrim("st", 100, false);
            events.assertNoFurtherNotifications();
        }
    }
}
