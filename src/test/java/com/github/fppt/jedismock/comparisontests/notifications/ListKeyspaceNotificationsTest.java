package com.github.fppt.jedismock.comparisontests.notifications;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.args.ListPosition;

import static com.github.fppt.jedismock.comparisontests.notifications.NotificationCollector.collectorFor;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Keyspace notifications of the list ({@code l}) event class.
 * <p>
 * A multi-element push reports one event, not one per element, and the
 * {@code X} variants report the same event as their unconditional forms.
 * Emptying a list additionally reports a <em>generic</em> {@code del}.
 */
@ExtendWith(ComparisonBase.class)
public class ListKeyspaceNotificationsTest {

    @TestTemplate
    public void pushCommandsPublishOneEventPerCommand(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "El")) {
            jedis.rpush("L", "a", "b", "c");
            jedis.lpush("L", "z");
            jedis.lpushx("L", "y");
            jedis.rpushx("L", "w");
            //Neither X variant does anything to a missing key, so neither reports
            jedis.lpushx("missing", "v");
            jedis.rpushx("missing", "v");
            assertThat(events.next(4)).containsExactly(
                    "__keyevent@0__:rpush -> L",
                    "__keyevent@0__:lpush -> L",
                    "__keyevent@0__:lpush -> L",
                    "__keyevent@0__:rpush -> L");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void popCommandsPublishPopThenGenericDelWhenEmptied(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Elg")) {
            jedis.rpush("L", "a", "b");
            jedis.lpop("L");
            jedis.rpop("L");
            assertThat(events.next(4)).containsExactly(
                    "__keyevent@0__:rpush -> L",
                    "__keyevent@0__:lpop -> L",
                    //the second pop empties the list, which is a generic 'del'
                    "__keyevent@0__:rpop -> L",
                    "__keyevent@0__:del -> L");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void blockingPopPublishesTheSamePopEvent(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "El")) {
            jedis.rpush("B", "a", "b");
            jedis.blpop(0, "B");
            jedis.brpop(0, "B");
            assertThat(events.next(3)).containsExactly(
                    "__keyevent@0__:rpush -> B",
                    "__keyevent@0__:lpop -> B",
                    "__keyevent@0__:rpop -> B");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void inPlaceMutationsPublishTheirOwnEvents(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "El")) {
            jedis.rpush("L", "a", "b", "c");
            jedis.linsert("L", ListPosition.BEFORE, "a", "q");
            jedis.lset("L", 0, "w");
            jedis.lrem("L", 0, "w");
            jedis.ltrim("L", 0, 0);
            assertThat(events.next(5)).containsExactly(
                    "__keyevent@0__:rpush -> L",
                    "__keyevent@0__:linsert -> L",
                    "__keyevent@0__:lset -> L",
                    "__keyevent@0__:lrem -> L",
                    "__keyevent@0__:ltrim -> L");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void rpoplpushPublishesDestinationPushBeforeSourcePop(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Elg")) {
            jedis.rpush("src", "a");
            jedis.rpoplpush("src", "dst");
            //Redis reports the push into the destination first, then the pop
            //from the source, then the generic del for the emptied source
            assertThat(events.next(4)).containsExactly(
                    "__keyevent@0__:rpush -> src",
                    "__keyevent@0__:lpush -> dst",
                    "__keyevent@0__:rpop -> src",
                    "__keyevent@0__:del -> src");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void sortWithStorePublishesSortstoreForTheDestination(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "El")) {
            jedis.rpush("unsorted", "3", "1", "2");
            jedis.sort("unsorted", "sorted");
            assertThat(events.next(2)).containsExactly(
                    "__keyevent@0__:rpush -> unsorted",
                    "__keyevent@0__:sortstore -> sorted");
            //A SORT without STORE writes nothing, so reports nothing
            jedis.sort("unsorted");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void listClassAloneMasksOtherClasses(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        //Mirrors the upstream "we are able to mask events" test
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "KEl")) {
            jedis.set("foo", "bar");
            jedis.lpush("mylist", "a");
            assertThat(events.next(2)).containsExactly(
                    "__keyspace@0__:mylist -> lpush",
                    "__keyevent@0__:lpush -> mylist");
            events.assertNoFurtherNotifications();
        }
    }
}
