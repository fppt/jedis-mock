package com.github.fppt.jedismock.comparisontests.notifications;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ZRangeParams;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.github.fppt.jedismock.comparisontests.notifications.NotificationCollector.collectorFor;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Keyspace notifications of the set ({@code s}), sorted set ({@code z}) and
 * hash ({@code h}) event classes.
 * <p>
 * As with lists, emptying a container additionally reports a <em>generic</em>
 * {@code del}, and a command that changes nothing reports nothing.
 */
@ExtendWith(ComparisonBase.class)
public class CollectionKeyspaceNotificationsTest {

    @TestTemplate
    public void setMutationsPublishTheirEvents(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Es")) {
            jedis.sadd("myset", "a", "b", "c", "d");
            //Adding a member that is already present changes nothing
            jedis.sadd("myset", "a");
            //Removing a member that is not there changes nothing
            jedis.srem("myset", "x");
            jedis.sadd("myset", "x", "y", "z");
            jedis.srem("myset", "x");
            //SPOP removes a random member, so it goes last to keep this
            //sequence deterministic
            jedis.spop("myset");
            assertThat(events.next(4)).containsExactly(
                    "__keyevent@0__:sadd -> myset",
                    "__keyevent@0__:sadd -> myset",
                    "__keyevent@0__:srem -> myset",
                    "__keyevent@0__:spop -> myset");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void popWithAZeroCountPublishesNothing(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        //A pop of zero elements modifies nothing, so it is not reported
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Eszg")) {
            jedis.sadd("s", "a", "b");
            jedis.zadd("z", 1, "a");
            assertThat(events.next(2)).containsExactly(
                    "__keyevent@0__:sadd -> s",
                    "__keyevent@0__:zadd -> z");
            jedis.spop("s", 0);
            jedis.zpopmin("z", 0);
            jedis.zpopmax("z", 0);
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void popOfAMissingKeyPublishesNothing(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Eszg")) {
            jedis.spop("nosuchset");
            jedis.spop("nosuchset", 2);
            jedis.zpopmin("nosuchzset");
            jedis.zpopmax("nosuchzset", 2);
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void setStoreCommandsPublishForTheDestination(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Es")) {
            jedis.sadd("s1", "a", "b");
            jedis.sadd("s2", "b", "c");
            jedis.sinterstore("d1", "s1", "s2");
            jedis.sunionstore("d2", "s1", "s2");
            jedis.sdiffstore("d3", "s1", "s2");
            assertThat(events.next(5)).containsExactly(
                    "__keyevent@0__:sadd -> s1",
                    "__keyevent@0__:sadd -> s2",
                    "__keyevent@0__:sinterstore -> d1",
                    "__keyevent@0__:sunionstore -> d2",
                    "__keyevent@0__:sdiffstore -> d3");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void smovePublishesSremThenSadd(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Esg")) {
            jedis.sadd("src", "only");
            jedis.sadd("dst", "other");
            jedis.smove("src", "dst", "only");
            assertThat(events.next(5)).containsExactly(
                    "__keyevent@0__:sadd -> src",
                    "__keyevent@0__:sadd -> dst",
                    //The source is fully reported (including the del for the
                    //container it emptied) before the destination — the
                    //opposite order to RPOPLPUSH
                    "__keyevent@0__:srem -> src",
                    "__keyevent@0__:del -> src",
                    "__keyevent@0__:sadd -> dst");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void emptyingASetPublishesGenericDel(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Esg")) {
            jedis.sadd("e1", "only");
            jedis.spop("e1");
            jedis.sadd("e2", "only");
            jedis.srem("e2", "only");
            assertThat(events.next(6)).containsExactly(
                    "__keyevent@0__:sadd -> e1",
                    "__keyevent@0__:spop -> e1",
                    "__keyevent@0__:del -> e1",
                    "__keyevent@0__:sadd -> e2",
                    "__keyevent@0__:srem -> e2",
                    "__keyevent@0__:del -> e2");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void sortedSetMutationsPublishTheirEvents(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Ez")) {
            jedis.zadd("myzset", 1, "a");
            jedis.zadd("myzset", 2, "b");
            //ZINCRBY reports 'zincr', not 'zincrby'
            jedis.zincrby("myzset", 1, "a");
            jedis.zrem("myzset", "nosuchmember");
            jedis.zrem("myzset", "a");
            jedis.zadd("myzset", 3, "c");
            jedis.zremrangeByScore("myzset", 3, 3);
            jedis.zadd("myzset", 4, "d");
            jedis.zremrangeByRank("myzset", 0, 0);
            jedis.zadd("myzset", 5, "lex");
            jedis.zremrangeByLex("myzset", "[lex", "[lex");
            assertThat(events.next(9)).containsExactly(
                    "__keyevent@0__:zadd -> myzset",
                    "__keyevent@0__:zadd -> myzset",
                    "__keyevent@0__:zincr -> myzset",
                    "__keyevent@0__:zrem -> myzset",
                    "__keyevent@0__:zadd -> myzset",
                    "__keyevent@0__:zremrangebyscore -> myzset",
                    "__keyevent@0__:zadd -> myzset",
                    "__keyevent@0__:zremrangebyrank -> myzset",
                    "__keyevent@0__:zadd -> myzset");
            assertThat(events.next(1)).containsExactly(
                    "__keyevent@0__:zremrangebylex -> myzset");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void sortedSetStoreAndPopCommandsPublishTheirEvents(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Ez")) {
            jedis.zadd("z1", 1, "a");
            jedis.zadd("z2", 2, "b");
            jedis.zunionstore("u", "z1", "z2");
            jedis.zinterstore("i", "z1", "z1");
            jedis.zrangestore("r", "z1", ZRangeParams.zrangeParams(0, -1));
            jedis.zpopmin("u");
            jedis.zpopmax("u");
            assertThat(events.next(7)).containsExactly(
                    "__keyevent@0__:zadd -> z1",
                    "__keyevent@0__:zadd -> z2",
                    "__keyevent@0__:zunionstore -> u",
                    "__keyevent@0__:zinterstore -> i",
                    "__keyevent@0__:zrangestore -> r",
                    "__keyevent@0__:zpopmin -> u",
                    "__keyevent@0__:zpopmax -> u");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void hashMutationsPublishTheirEvents(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Eh")) {
            Map<String, String> fields = new LinkedHashMap<>();
            fields.put("yes", "1");
            fields.put("no", "0");
            //All of HSET, HMSET and HSETNX report 'hset'
            jedis.hmset("myhash", fields);
            jedis.hset("myhash", "other", "v");
            jedis.hsetnx("myhash", "fresh", "v");
            //HSETNX on an existing field changes nothing
            jedis.hsetnx("myhash", "fresh", "w");
            jedis.hincrBy("myhash", "yes", 10);
            jedis.hincrByFloat("myhash", "yes", 1.5);
            jedis.hdel("myhash", "nosuchfield");
            jedis.hdel("myhash", "other");
            assertThat(events.next(6)).containsExactly(
                    "__keyevent@0__:hset -> myhash",
                    "__keyevent@0__:hset -> myhash",
                    "__keyevent@0__:hset -> myhash",
                    "__keyevent@0__:hincrby -> myhash",
                    "__keyevent@0__:hincrbyfloat -> myhash",
                    "__keyevent@0__:hdel -> myhash");
            events.assertNoFurtherNotifications();
        }
    }

    @TestTemplate
    public void emptyingAHashOrSortedSetPublishesGenericDel(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "Ehzg")) {
            jedis.hset("eh", "f", "v");
            jedis.hdel("eh", "f");
            jedis.zadd("ez", 1, "only");
            jedis.zrem("ez", "only");
            assertThat(events.next(6)).containsExactly(
                    "__keyevent@0__:hset -> eh",
                    "__keyevent@0__:hdel -> eh",
                    "__keyevent@0__:del -> eh",
                    "__keyevent@0__:zadd -> ez",
                    "__keyevent@0__:zrem -> ez",
                    "__keyevent@0__:del -> ez");
            events.assertNoFurtherNotifications();
        }
    }
}
