package com.github.fppt.jedismock.comparisontests.notifications;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.GetExParams;
import redis.clients.jedis.params.SetParams;

import static com.github.fppt.jedismock.comparisontests.notifications.NotificationCollector.collectorFor;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Keyspace notifications for the generic ({@code g}) and expired ({@code x})
 * event classes: which commands publish which event, on which of the two
 * channel families, and for which database.
 * <p>
 * Note on {@code EXPIRE} with a time in the past: Redis deletes the key and
 * publishes a generic {@code del}, whereas Valkey publishes {@code expired}.
 * These tests follow Redis, matching the container they run against.
 */
@ExtendWith(ComparisonBase.class)
public class GenericKeyspaceNotificationsTest {

    @TestTemplate
    public void generalEventsTest(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "KEg")) {
            jedis.set("foo", "bar");   //string class: silent under 'g'
            jedis.expire("foo", 100);
            jedis.del("foo");
            assertThat(events.next(4)).containsExactly(
                    "__keyspace@0__:foo -> expire",
                    "__keyevent@0__:expire -> foo",
                    "__keyspace@0__:foo -> del",
                    "__keyevent@0__:del -> foo");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void keyspaceChannelOnly(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Kg")) {
            jedis.set("foo", "bar");
            jedis.del("foo");
            assertThat(events.next(1)).containsExactly("__keyspace@0__:foo -> del");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void keyeventChannelOnly(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Eg")) {
            jedis.set("foo", "bar");
            jedis.del("foo");
            assertThat(events.next(1)).containsExactly("__keyevent@0__:del -> foo");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void nothingIsPublishedWhileDisabled(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "")) {
            jedis.set("foo", "bar");
            jedis.expire("foo", 100);
            jedis.del("foo");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void deleteEventsForDelUnlinkAndGetdel(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Eg")) {
            jedis.mset("d1", "v", "d2", "v", "d3", "v");
            jedis.del("d1");
            jedis.unlink("d2");
            jedis.getDel("d3");
            //UNLINK publishes 'del' as well, not 'unlink'
            assertThat(events.next(3)).containsExactly(
                    "__keyevent@0__:del -> d1",
                    "__keyevent@0__:del -> d2",
                    "__keyevent@0__:del -> d3");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void deleteEventPerKeyOfMultiKeyDelete(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Eg")) {
            jedis.mset("m1", "v", "m2", "v");
            //Only existing keys are reported
            jedis.del("m1", "missing", "m2");
            assertThat(events.next(2)).containsExactly(
                    "__keyevent@0__:del -> m1",
                    "__keyevent@0__:del -> m2");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void expirationCommandsPublishExpire(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Eg")) {
            jedis.mset("e1", "v", "e2", "v", "e3", "v", "e4", "v");
            jedis.expire("e1", 100);
            jedis.pexpire("e2", 100_000);
            jedis.expireAt("e3", System.currentTimeMillis() / 1000 + 100);
            jedis.pexpireAt("e4", System.currentTimeMillis() + 100_000);
            assertThat(events.next(4)).containsExactly(
                    "__keyevent@0__:expire -> e1",
                    "__keyevent@0__:expire -> e2",
                    "__keyevent@0__:expire -> e3",
                    "__keyevent@0__:expire -> e4");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void expirationInThePastPublishesDelete(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Egx")) {
            jedis.mset("p1", "v", "p2", "v");
            jedis.expire("p1", -1);
            jedis.pexpireAt("p2", 1);
            //The key is deleted straight away: a 'del', not an 'expired'
            assertThat(events.next(2)).containsExactly(
                    "__keyevent@0__:del -> p1",
                    "__keyevent@0__:del -> p2");
            events.assertNoFurtherNotifications();
            assertThat(jedis.exists("p1")).isFalse();
        }
    }

    @TestTemplate
    public void noEventForExpirationOfMissingKey(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Egx")) {
            jedis.expire("nosuchkey", 100);
            jedis.expire("nosuchkey", -1);
            jedis.del("nosuchkey");
            jedis.persist("nosuchkey");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void persistPublishesPersistOnlyWhenTtlIsRemoved(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Eg")) {
            jedis.set("pers", "v", SetParams.setParams().ex(100));
            assertThat(events.next(1)).containsExactly("__keyevent@0__:expire -> pers");
            jedis.persist("pers");
            assertThat(events.next(1)).containsExactly("__keyevent@0__:persist -> pers");
            //No TTL left to remove: silent
            jedis.persist("pers");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void getexPublishesExpirePersistOrDelete(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        //GETEX reports only what it did to the expiration, and always as a
        //generic event -- never the string 'set', despite being a write command
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "KEg")) {
            jedis.set("gx", "v");
            jedis.getEx("gx", GetExParams.getExParams().ex(100));
            jedis.getEx("gx", GetExParams.getExParams().persist());
            jedis.getEx("gx", GetExParams.getExParams().exAt(1));
            assertThat(events.next(6)).containsExactly(
                    "__keyspace@0__:gx -> expire",
                    "__keyevent@0__:expire -> gx",
                    "__keyspace@0__:gx -> persist",
                    "__keyevent@0__:persist -> gx",
                    //An absolute deadline in the past deletes the key: a 'del'
                    "__keyspace@0__:gx -> del",
                    "__keyevent@0__:del -> gx");
            events.assertNoFurtherNotifications();
            assertThat(jedis.exists("gx")).isFalse();
        }
    }

    @TestTemplate
    public void getexIsSilentWhenItChangesNoExpiration(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Egx$n")) {
            jedis.set("gx", "v", SetParams.setParams().ex(100));
            assertThat(events.next(3)).containsExactly(
                    "__keyevent@0__:new -> gx",
                    "__keyevent@0__:set -> gx",
                    "__keyevent@0__:expire -> gx");
            //No option at all: a plain read, so nothing is published
            jedis.getEx("gx", GetExParams.getExParams());
            jedis.persist("gx");
            assertThat(events.next(1)).containsExactly("__keyevent@0__:persist -> gx");
            //PERSIST with no TTL left to remove, and GETEX on a missing key
            jedis.getEx("gx", GetExParams.getExParams().persist());
            jedis.getEx("nosuchkey", GetExParams.getExParams().ex(100));
            jedis.getEx("nosuchkey", GetExParams.getExParams().exAt(1));
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void getexEventsBelongToTheGenericClassAlone(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "E$")) {
            jedis.set("gx", "v");
            assertThat(events.next(1)).containsExactly("__keyevent@0__:set -> gx");
            //Under the string class alone none of GETEX's events get through
            jedis.getEx("gx", GetExParams.getExParams().ex(100));
            jedis.getEx("gx", GetExParams.getExParams().persist());
            jedis.getEx("gx", GetExParams.getExParams().pxAt(1));
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void settingWithExpirationPublishesGenericExpire(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Eg")) {
            //The accompanying 'set' belongs to the string class, so under 'g'
            //only the expiration is reported
            jedis.set("s1", "v", SetParams.setParams().ex(100));
            jedis.setex("s2", 100, "v");
            jedis.psetex("s3", 100_000, "v");
            assertThat(events.next(3)).containsExactly(
                    "__keyevent@0__:expire -> s1",
                    "__keyevent@0__:expire -> s2",
                    "__keyevent@0__:expire -> s3");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void renamePublishesRenameFromAndRenameTo(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "KEg")) {
            jedis.set("old", "v");
            jedis.rename("old", "new");
            assertThat(events.next(4)).containsExactly(
                    "__keyspace@0__:old -> rename_from",
                    "__keyevent@0__:rename_from -> old",
                    "__keyspace@0__:new -> rename_to",
                    "__keyevent@0__:rename_to -> new");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void movePublishesMoveFromAndMoveToOfDestinationDatabase(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "KEg")) {
            jedis.set("mv", "v");
            jedis.move("mv", 1);
            //move_to is published on the destination database's channels
            assertThat(events.next(4)).containsExactly(
                    "__keyspace@0__:mv -> move_from",
                    "__keyevent@0__:move_from -> mv",
                    "__keyspace@1__:mv -> move_to",
                    "__keyevent@1__:move_to -> mv");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void copyPublishesCopyToForDestinationOnly(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Eg")) {
            jedis.set("src", "v");
            jedis.copy("src", "dst", false);
            assertThat(events.next(1)).containsExactly("__keyevent@0__:copy_to -> dst");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void expiredEventOnAccessOfAnExpiredKey(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Ex")) {
            jedis.psetex("gone", 50, "v");
            Awaitility.await().until(() -> !jedis.exists("gone"));
            assertThat(events.next(1)).containsExactly("__keyevent@0__:expired -> gone");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void expiredEventIsPublishedOnceForAKeySweptByKeys(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Ex")) {
            jedis.psetex("swept", 50, "v");
            Awaitility.await().until(() -> jedis.keys("swept").isEmpty());
            assertThat(events.next(1)).containsExactly("__keyevent@0__:expired -> swept");
            events.assertNoFurtherNotifications();
        }
    }
}
