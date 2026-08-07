package com.github.fppt.jedismock.comparisontests.notifications;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import static com.github.fppt.jedismock.comparisontests.notifications.NotificationCollector.collectorFor;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Keyspace notifications of the string ({@code $}) event class.
 * <p>
 * The event name is not the command name: every flavour of assignment reports
 * {@code set}, and all four of {@code INCR}, {@code INCRBY}, {@code DECR} and
 * {@code DECRBY} report {@code incrby}.
 */
@ExtendWith(ComparisonBase.class)
public class StringKeyspaceNotificationsTest {

    @TestTemplate
    public void assignmentCommandsPublishSet(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "E$")) {
            jedis.set("s1", "v");
            jedis.setnx("s2", "v");
            jedis.mset("m1", "1", "m2", "2");
            jedis.msetnx("m3", "1", "m4", "2");
            jedis.getSet("s1", "w");
            jedis.setex("sx", 100, "v");
            assertThat(events.next(8)).containsExactly(
                    "__keyevent@0__:set -> s1",
                    "__keyevent@0__:set -> s2",
                    "__keyevent@0__:set -> m1",
                    "__keyevent@0__:set -> m2",
                    "__keyevent@0__:set -> m3",
                    "__keyevent@0__:set -> m4",
                    "__keyevent@0__:set -> s1",
                    "__keyevent@0__:set -> sx");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void bothChannelFamiliesCarryTheSetEvent(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "KE$")) {
            jedis.set("foo", "bar");
            assertThat(events.next(2)).containsExactly(
                    "__keyspace@0__:foo -> set",
                    "__keyevent@0__:set -> foo");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void settingWithExpirationPublishesSetThenExpire(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        //Both classes enabled: the string 'set' is reported before the generic 'expire'
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "E$g")) {
            jedis.set("withex", "v", SetParams.setParams().ex(100));
            jedis.setex("sx", 100, "v");
            jedis.psetex("px", 100_000, "v");
            assertThat(events.next(6)).containsExactly(
                    "__keyevent@0__:set -> withex",
                    "__keyevent@0__:expire -> withex",
                    "__keyevent@0__:set -> sx",
                    "__keyevent@0__:expire -> sx",
                    "__keyevent@0__:set -> px",
                    "__keyevent@0__:expire -> px");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void appendAndSetrangePublishTheirOwnEvents(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "E$")) {
            jedis.append("a1", "abc");
            jedis.append("a1", "def");
            jedis.setrange("a1", 0, "z");
            assertThat(events.next(3)).containsExactly(
                    "__keyevent@0__:append -> a1",
                    "__keyevent@0__:append -> a1",
                    "__keyevent@0__:setrange -> a1");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void allIntegerIncrementsPublishIncrby(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "E$")) {
            //Redis reports 'incrby' for the decrementing variants too
            jedis.incr("c1");
            jedis.incrBy("c1", 5);
            jedis.decr("c1");
            jedis.decrBy("c1", 2);
            jedis.incrByFloat("f1", 1.5);
            assertThat(events.next(5)).containsExactly(
                    "__keyevent@0__:incrby -> c1",
                    "__keyevent@0__:incrby -> c1",
                    "__keyevent@0__:incrby -> c1",
                    "__keyevent@0__:incrby -> c1",
                    "__keyevent@0__:incrbyfloat -> f1");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void conditionalAssignmentsThatDoNothingAreSilent(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "E$")) {
            jedis.set("exists", "v");
            assertThat(events.next(1)).containsExactly("__keyevent@0__:set -> exists");
            //None of these change anything, so none are reported
            jedis.setnx("exists", "other");
            jedis.msetnx("exists", "other", "fresh", "v");
            jedis.set("missing", "v", SetParams.setParams().xx());
            jedis.set("exists", "v", SetParams.setParams().nx());
            //GET reports the previous value but does not make the write happen
            jedis.setGet("exists", "other", SetParams.setParams().nx());
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void stringClassAloneMasksOtherClasses(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "E$")) {
            jedis.set("masked", "v");
            assertThat(events.next(1)).containsExactly("__keyevent@0__:set -> masked");
            //DEL and EXPIRE are generic, which is not enabled
            jedis.expire("masked", 100);
            jedis.del("masked");
            events.assertNoFurtherNotifications();
        }
    }
}
