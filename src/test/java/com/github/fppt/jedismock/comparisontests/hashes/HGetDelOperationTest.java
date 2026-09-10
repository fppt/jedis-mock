package com.github.fppt.jedismock.comparisontests.hashes;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import com.github.fppt.jedismock.comparisontests.notifications.NotificationCollector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static com.github.fppt.jedismock.comparisontests.notifications.NotificationCollector.collectorFor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import static org.assertj.core.api.InstanceOfAssertFactories.LIST;

@ExtendWith(ComparisonBase.class)
public class HGetDelOperationTest {

    private final String HASH = "hash";
    private final String FIELD_1 = "field1";
    private final String VALUE_1 = "value1";
    private final String FIELD_2 = "field2";
    private final String VALUE_2 = "value2";
    private final String FIELD_3 = "field3";
    private final String VALUE_3 = "value3";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void whenHGetDelSingleField_EnsureDeletedValueIsReturned(Jedis jedis) {
        jedis.hset(HASH, FIELD_1, VALUE_1);
        jedis.hset(HASH, FIELD_2, VALUE_2);

        assertThat(jedis.hgetdel(HASH, FIELD_1)).containsExactly(VALUE_1);
        assertThat(jedis.hexists(HASH, FIELD_1)).isFalse();
        assertThat(jedis.hexists(HASH, FIELD_2)).isTrue();
        assertThat(jedis.hget(HASH, FIELD_2)).isEqualTo(VALUE_2);
    }

    @TestTemplate
    public void whenHGetDelMultipleFields_EnsureDeletedValuesAreReturned(Jedis jedis) {
        jedis.hset(HASH, FIELD_1, VALUE_1);
        jedis.hset(HASH, FIELD_2, VALUE_2);
        jedis.hset(HASH, FIELD_3, VALUE_3);

        assertThat(jedis.hgetdel(HASH, FIELD_1, FIELD_3)).containsExactly(VALUE_1, VALUE_3);
        assertThat(jedis.hexists(HASH, FIELD_1)).isFalse();
        assertThat(jedis.hexists(HASH, FIELD_2)).isTrue();
        assertThat(jedis.hexists(HASH, FIELD_3)).isFalse();
        assertThat(jedis.hget(HASH, FIELD_2)).isEqualTo(VALUE_2);
    }

    @TestTemplate
    public void hGetDelWithNoFields(Jedis jedis) {
        jedis.hset(HASH, FIELD_1, VALUE_1);
        jedis.hset(HASH, FIELD_2, VALUE_2);
        jedis.hset(HASH, FIELD_3, VALUE_3);

        assertThatThrownBy(() -> jedis.hgetdel(HASH))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR wrong number of arguments for 'hgetdel' command");
        assertThat(jedis.hgetAll(HASH)).containsExactlyInAnyOrderEntriesOf(Map.of(FIELD_1, VALUE_1, FIELD_2, VALUE_2, FIELD_3, VALUE_3));
    }

    @TestTemplate
    public void hGetDelIgnoresFieldsArgCasing(Jedis jedis) {
        jedis.hset(HASH, FIELD_1, VALUE_1);
        jedis.hset(HASH, FIELD_2, VALUE_2);
        jedis.hset(HASH, FIELD_3, VALUE_3);

        assertThat(jedis.sendCommand(Protocol.Command.HGETDEL, HASH, "FiElDs", "2", FIELD_1, FIELD_3))
                .asInstanceOf(LIST)
                .containsExactly(VALUE_1.getBytes(StandardCharsets.UTF_8), VALUE_3.getBytes(StandardCharsets.UTF_8));
        assertThat(jedis.hexists(HASH, FIELD_1)).isFalse();
        assertThat(jedis.hexists(HASH, FIELD_2)).isTrue();
        assertThat(jedis.hexists(HASH, FIELD_3)).isFalse();
        assertThat(jedis.hget(HASH, FIELD_2)).isEqualTo(VALUE_2);
    }

    @TestTemplate
    public void hGetDelReturnsErrorWhenFieldsArgIsIncorrect(Jedis jedis) {
        jedis.hset(HASH, FIELD_1, VALUE_1);
        jedis.hset(HASH, FIELD_2, VALUE_2);
        jedis.hset(HASH, FIELD_3, VALUE_3);

        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HGETDEL, HASH, "NOTFIELDS", "2", FIELD_1, FIELD_3))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Mandatory argument FIELDS is missing or not at the right position");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HGETDEL, HASH))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR wrong number of arguments for 'hgetdel' command");
        assertThat(jedis.hgetAll(HASH)).containsExactlyInAnyOrderEntriesOf(Map.of(FIELD_1, VALUE_1, FIELD_2, VALUE_2, FIELD_3, VALUE_3));
    }

    @TestTemplate
    public void hGetDelReturnsErrorWhenNumFieldsIsIncorrect(Jedis jedis) {
        jedis.hset(HASH, FIELD_1, VALUE_1);
        jedis.hset(HASH, FIELD_2, VALUE_2);
        jedis.hset(HASH, FIELD_3, VALUE_3);

        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HGETDEL, HASH, "FIELDS"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR wrong number of arguments for 'hgetdel' command");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HGETDEL, HASH, "FIELDS", "not a number", FIELD_1, FIELD_3))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Number of fields must be a positive integer");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HGETDEL, HASH, "FIELDS", "-1", FIELD_1, FIELD_3))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Number of fields must be a positive integer");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HGETDEL, HASH, "FIELDS", "0", FIELD_1, FIELD_3))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Number of fields must be a positive integer");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HGETDEL, HASH, "FIELDS", "1", FIELD_1, FIELD_3))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR The `numfields` parameter must match the number of arguments");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HGETDEL, HASH, "FIELDS", "3", FIELD_1, FIELD_3))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR The `numfields` parameter must match the number of arguments");
        assertThat(jedis.hgetAll(HASH)).containsExactlyInAnyOrderEntriesOf(Map.of(FIELD_1, VALUE_1, FIELD_2, VALUE_2, FIELD_3, VALUE_3));
    }

    @TestTemplate
    public void hGetDelReturnsNilForFieldsThatDidNotExist(Jedis jedis) {
        jedis.hset(HASH, FIELD_1, VALUE_1);
        jedis.hset(HASH, FIELD_2, VALUE_2);

        assertThat(jedis.hgetdel(HASH, FIELD_3, FIELD_1)).containsExactly(null, VALUE_1);
        assertThat(jedis.hexists(HASH, FIELD_1)).isFalse();
        assertThat(jedis.hexists(HASH, FIELD_2)).isTrue();
        assertThat(jedis.hexists(HASH, FIELD_3)).isFalse();
        assertThat(jedis.hget(HASH, FIELD_2)).isEqualTo(VALUE_2);
    }

    @TestTemplate
    public void hGetDelDeletesEntireKeyIfAllFieldsAreDeleted(Jedis jedis) {
        jedis.hset(HASH, FIELD_1, VALUE_1);
        jedis.hset(HASH, FIELD_2, VALUE_2);

        assertThat(jedis.hgetdel(HASH, FIELD_1, FIELD_2)).containsExactly(VALUE_1, VALUE_2);
        assertThat(jedis.hexists(HASH, FIELD_1)).isFalse();
        assertThat(jedis.hexists(HASH, FIELD_2)).isFalse();
        assertThat(jedis.exists(HASH)).isFalse();
    }

    @TestTemplate
    public void hGetDelPublishesKeyspaceNotifications(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        jedis.hset(HASH, FIELD_1, VALUE_1);
        jedis.hset(HASH, FIELD_2, VALUE_2);
        jedis.hset(HASH, FIELD_3, VALUE_3);

        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "KEgh")) {
            assertThat(jedis.hgetdel(HASH, FIELD_1, FIELD_3)).containsExactly(VALUE_1, VALUE_3);
            assertThat(events.next(2)).containsExactly(
                    "__keyspace@0__:" + HASH + " -> hdel",
                    "__keyevent@0__:hdel -> " + HASH);

            assertThat(jedis.hgetdel(HASH, FIELD_2)).containsExactly(VALUE_2);
            assertThat(events.next(4)).containsExactly(
                    "__keyspace@0__:" + HASH + " -> hdel",
                    "__keyevent@0__:hdel -> " + HASH,
                    "__keyspace@0__:" + HASH + " -> del",
                    "__keyevent@0__:del -> " + HASH);

            events.assertNoFurtherNotifications();
        }
    }
}
