package com.github.fppt.jedismock.comparisontests.notifications;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.params.ZRangeParams;

import static com.github.fppt.jedismock.comparisontests.notifications.NotificationCollector.collectorFor;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Edge cases around which command variants publish which event, gathered while
 * reviewing the type-class implementation: commands that report a different
 * event depending on an option, commands whose source and destination are the
 * same key, and writes whose result happens to equal what was already stored.
 */
@ExtendWith(ComparisonBase.class)
public class KeyspaceNotificationEdgeCasesTest {

    @TestTemplate
    public void zaddWithIncrOptionPublishesZincr(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Ez")) {
            jedis.zadd("z", 1, "m");
            //ZADD ... INCR is an increment, and is reported as one
            jedis.zaddIncr("z", 5, "m", ZAddParams.zAddParams());
            assertThat(events.next(2)).containsExactly(
                    "__keyevent@0__:zadd -> z",
                    "__keyevent@0__:zincr -> z");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void movingAMemberOntoItsOwnSetPublishesNothing(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Esg")) {
            jedis.sadd("s", "m");
            assertThat(events.next(1)).containsExactly("__keyevent@0__:sadd -> s");
            //Source and destination are the same key: nothing changes
            assertThat(jedis.smove("s", "s", "m")).isEqualTo(1L);
            events.assertNoFurtherNotifications();
            assertThat(jedis.smembers("s")).containsExactly("m");
        }
    }

    @TestTemplate
    public void rangestoreFromAMissingSourcePublishesDelForTheDestination(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Ezg")) {
            jedis.zadd("dest", 1, "m");
            assertThat(events.next(1)).containsExactly("__keyevent@0__:zadd -> dest");
            //An empty result removes the destination, which is a generic del
            jedis.zrangestore("dest", "nosuchkey", ZRangeParams.zrangeParams(0, -1));
            assertThat(events.next(1)).containsExactly("__keyevent@0__:del -> dest");
            events.assertNoFurtherNotifications();
            assertThat(jedis.exists("dest")).isFalse();
        }
    }

    @TestTemplate
    public void setrangeWritingTheSameBytesStillPublishes(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "E$")) {
            jedis.set("k", "abc");
            assertThat(events.next(1)).containsExactly("__keyevent@0__:set -> k");
            //The stored value does not change, but the command still writes
            jedis.setrange("k", 0, "abc");
            assertThat(events.next(1)).containsExactly("__keyevent@0__:setrange -> k");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void setrangeWithAnEmptyValuePublishesNothing(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "E$")) {
            jedis.set("k", "abc");
            assertThat(events.next(1)).containsExactly("__keyevent@0__:set -> k");
            //An empty value writes nothing at all
            jedis.setrange("k", 0, "");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void appendOfAnEmptyValueStillPublishes(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "E$")) {
            jedis.set("k", "abc");
            assertThat(events.next(1)).containsExactly("__keyevent@0__:set -> k");
            jedis.append("k", "");
            assertThat(events.next(1)).containsExactly("__keyevent@0__:append -> k");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void rotatingAListOntoItselfPublishesBothEnds(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Elg")) {
            jedis.rpush("l", "a", "b");
            assertThat(events.next(1)).containsExactly("__keyevent@0__:rpush -> l");
            //Source and destination are the same list
            jedis.rpoplpush("l", "l");
            assertThat(events.next(2)).containsExactly(
                    "__keyevent@0__:lpush -> l",
                    "__keyevent@0__:rpop -> l");
            events.assertNoFurtherNotifications();
        }
    }
}
