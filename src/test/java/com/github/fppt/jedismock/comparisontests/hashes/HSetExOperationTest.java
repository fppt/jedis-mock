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

import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.fppt.jedismock.comparisontests.notifications.NotificationCollector.collectorFor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static redis.clients.jedis.params.HSetExParams.hSetExParams;

@ExtendWith(ComparisonBase.class)
public class HSetExOperationTest {

    private final String HASH = "hash";
    private final String FIELD_1 = "field1";
    private final String VALUE_1 = "value1";
    private final String FIELD_2 = "field2";
    private final String VALUE_2 = "value2";
    private final String FIELD_3 = "field3";
    private final String VALUE_3 = "value3";
    private final String FIELD_4 = "field4";
    private final String VALUE_4 = "value4";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void hSetExCanUnconditionallySetASingleFieldWithNoExpiry(Jedis jedis) {
        assertThat(jedis.hgetAll(HASH)).isEmpty();

        assertThat(jedis.hsetex(HASH, hSetExParams(), FIELD_1, VALUE_1)).isEqualTo(1);

        assertThat(jedis.hgetAll(HASH)).containsOnly(entry(FIELD_1, VALUE_1));
        assertThat(jedis.httl(HASH, FIELD_1)).containsExactly(-1L);
    }

    @TestTemplate
    public void hSetExCanUnconditionallySetMultipleFieldsWithNoExpiry(Jedis jedis) {
        assertThat(jedis.hgetAll(HASH)).isEmpty();

        assertThat(jedis.hsetex(HASH, hSetExParams(), Map.of(FIELD_1, VALUE_1, FIELD_2, VALUE_2))).isEqualTo(1);

        assertThat(jedis.hgetAll(HASH)).containsOnly(entry(FIELD_1, VALUE_1), entry(FIELD_2, VALUE_2));
        assertThat(jedis.httl(HASH, FIELD_1, FIELD_2)).containsOnly(-1L);
    }

    @TestTemplate
    public void hSetExCanUnconditionallySetMultipleFieldsWithExpiry(Jedis jedis) {
        assertThat(List.of(
                hSetExParams().ex(100),
                hSetExParams().exAt(Instant.now().getEpochSecond() + 100),
                hSetExParams().px(100 * 1000),
                hSetExParams().pxAt(Instant.now().toEpochMilli() + 100 * 1000)
        )).allSatisfy((params) -> {
            jedis.flushAll();

            assertThat(jedis.hsetex(HASH, params, Map.of(FIELD_1, VALUE_1, FIELD_2, VALUE_2))).isEqualTo(1);

            assertThat(jedis.hgetAll(HASH)).containsOnly(entry(FIELD_1, VALUE_1), entry(FIELD_2, VALUE_2));
            assertThat(jedis.httl(HASH, FIELD_1, FIELD_2)).allSatisfy((ttl) -> assertThat(ttl).isBetween(90L, 101L));
        });
    }

    @TestTemplate
    public void hSetExCanUnconditionallySetMultipleFieldsWithoutAlteringExistingTtls(Jedis jedis) {
        jedis.hset(HASH, Map.of(FIELD_1, VALUE_1, FIELD_2, VALUE_2, FIELD_3, VALUE_3));
        jedis.hexpire(HASH, 100, FIELD_1);
        jedis.hexpire(HASH, 200, FIELD_3);
        assertThat(jedis.httl(HASH, FIELD_1, FIELD_2, FIELD_3, FIELD_4)).satisfiesExactly(
                (ttl) -> assertThat(ttl).isBetween(90L, 101L),
                (ttl) -> assertThat(ttl).isEqualTo(-1),
                (ttl) -> assertThat(ttl).isBetween(190L, 201L),
                (ttl) -> assertThat(ttl).isEqualTo(-2)
        );

        assertThat(jedis.hsetex(HASH, hSetExParams().keepTtl(), Map.of(FIELD_1, VALUE_1, FIELD_2, VALUE_2, FIELD_4, VALUE_4))).isEqualTo(1);

        assertThat(jedis.hgetAll(HASH)).containsOnly(entry(FIELD_1, VALUE_1), entry(FIELD_2, VALUE_2), entry(FIELD_3, VALUE_3), entry(FIELD_4, VALUE_4));
        assertThat(jedis.httl(HASH, FIELD_1, FIELD_2, FIELD_3, FIELD_4)).satisfiesExactly(
                (ttl) -> assertThat(ttl).isBetween(80L, 101L),
                (ttl) -> assertThat(ttl).isEqualTo(-1),
                (ttl) -> assertThat(ttl).isBetween(180L, 201L),
                (ttl) -> assertThat(ttl).isEqualTo(-1)
        );
        assertThat(jedis.ttl(HASH)).isEqualTo(-1);
    }

    @TestTemplate
    public void hSetExErasesExistingTtlsByDefault(Jedis jedis) {
        jedis.hset(HASH, Map.of(FIELD_1, VALUE_1, FIELD_2, VALUE_2, FIELD_3, VALUE_3));
        jedis.hexpire(HASH, 100, FIELD_1);
        jedis.hexpire(HASH, 200, FIELD_3);
        assertThat(jedis.httl(HASH, FIELD_1, FIELD_2, FIELD_3, FIELD_4)).satisfiesExactly(
                (ttl) -> assertThat(ttl).isBetween(90L, 101L),
                (ttl) -> assertThat(ttl).isEqualTo(-1),
                (ttl) -> assertThat(ttl).isBetween(190L, 201L),
                (ttl) -> assertThat(ttl).isEqualTo(-2)
        );

        assertThat(jedis.hsetex(HASH, hSetExParams(), Map.of(FIELD_1, VALUE_1, FIELD_2, VALUE_2, FIELD_4, VALUE_4))).isEqualTo(1);

        assertThat(jedis.hgetAll(HASH)).containsOnly(entry(FIELD_1, VALUE_1), entry(FIELD_2, VALUE_2), entry(FIELD_3, VALUE_3), entry(FIELD_4, VALUE_4));
        assertThat(jedis.httl(HASH, FIELD_1, FIELD_2, FIELD_3, FIELD_4)).satisfiesExactly(
                (ttl) -> assertThat(ttl).isEqualTo(-1),
                (ttl) -> assertThat(ttl).isEqualTo(-1),
                (ttl) -> assertThat(ttl).isBetween(180L, 201L),
                (ttl) -> assertThat(ttl).isEqualTo(-1)
        );
    }

    @TestTemplate
    public void hSetExDeletesFieldsWithZeroOrPastExpiry(Jedis jedis) {
        assertThat(List.of(
                hSetExParams().ex(0),
                hSetExParams().px(0),
                hSetExParams().exAt(0),
                hSetExParams().pxAt(0),
                hSetExParams().exAt(Instant.now().getEpochSecond()),
                hSetExParams().pxAt(Instant.now().toEpochMilli())
        )).allSatisfy(params -> {
            jedis.hset(HASH, FIELD_1, VALUE_1);
            assertThat(jedis.hexists(HASH, FIELD_1)).isTrue();

            assertThat(jedis.hsetex(HASH, params, FIELD_1, VALUE_2)).isEqualTo(1);

            assertThat(jedis.hexists(HASH, FIELD_1)).isFalse();
        });
    }

    @TestTemplate
    public void hSetExCanSetFieldsOnlyIfNoneOfThemExist(Jedis jedis) {
        assertThat(List.of(
                Map.<String, String>of(),
                Map.of(FIELD_2, VALUE_2),
                Map.of(FIELD_2, VALUE_1)
        )).allSatisfy((initialValues) -> {
            jedis.flushAll();
            if (!initialValues.isEmpty()) jedis.hset(HASH, initialValues);

            assertThat(jedis.hsetex(HASH, hSetExParams().fnx(), Map.of(FIELD_1, VALUE_1, FIELD_3, VALUE_3))).isEqualTo(1);

            assertThat(jedis.hgetAll(HASH))
                    .hasSize(initialValues.size() + 2)
                    .containsAllEntriesOf(initialValues)
                    .containsAllEntriesOf(Map.of(FIELD_1, VALUE_1, FIELD_3, VALUE_3));
        });

        assertThat(List.of(
                Map.of(FIELD_1, VALUE_1),
                Map.of(FIELD_1, VALUE_2),
                Map.of(FIELD_3, VALUE_2),
                Map.of(FIELD_1, VALUE_1, FIELD_3, VALUE_3),
                Map.of(FIELD_1, VALUE_1, FIELD_2, VALUE_2, FIELD_3, VALUE_3)
        )).allSatisfy((initialValues) -> {
            jedis.flushAll();
            jedis.hset(HASH, initialValues);

            assertThat(jedis.hsetex(HASH, hSetExParams().fnx(), Map.of(FIELD_1, VALUE_1, FIELD_3, VALUE_3))).isEqualTo(0);

            assertThat(jedis.hgetAll(HASH)).containsExactlyInAnyOrderEntriesOf(initialValues);
        });
    }

    @TestTemplate
    public void hSetExCanSetFieldsOnlyIfAllOfThemExist(Jedis jedis) {
        assertThat(List.of(
                Map.<String, String>of(),
                Map.of(FIELD_2, VALUE_2),
                Map.of(FIELD_2, VALUE_1),
                Map.of(FIELD_3, VALUE_3)
        )).allSatisfy((initialValues) -> {
            jedis.flushAll();
            if (!initialValues.isEmpty()) jedis.hset(HASH, initialValues);

            assertThat(jedis.hsetex(HASH, hSetExParams().fxx(), Map.of(FIELD_1, VALUE_1, FIELD_3, VALUE_3))).isEqualTo(0);

            assertThat(jedis.hgetAll(HASH)).containsExactlyInAnyOrderEntriesOf(initialValues);
        });

        assertThat(List.of(
                Map.of(FIELD_1, VALUE_1, FIELD_3, VALUE_3),
                Map.of(FIELD_1, VALUE_2, FIELD_3, VALUE_2),
                Map.of(FIELD_1, VALUE_1, FIELD_2, VALUE_2, FIELD_3, VALUE_3)
        )).allSatisfy((initialValues) -> {
            jedis.flushAll();
            jedis.hset(HASH, initialValues);

            Map<String, String> newValues = Map.of(FIELD_1, VALUE_1, FIELD_3, VALUE_3);
            assertThat(jedis.hsetex(HASH, hSetExParams().fxx(), newValues)).isEqualTo(1);

            Map<String, String> expectedValues = new HashMap<>(initialValues);
            expectedValues.putAll(newValues);
            assertThat(jedis.hgetAll(HASH)).containsExactlyInAnyOrderEntriesOf(expectedValues);
        });
    }

    @TestTemplate
    public void hSetExWithNoFields(Jedis jedis) {
        assertThat(jedis.hgetAll(HASH)).isEmpty();

        assertThat(List.of(
                hSetExParams(),
                hSetExParams().fnx().ex(1)
        )).allSatisfy(params ->
                assertThatThrownBy(() -> jedis.hsetex(HASH, params, Map.of()))
                        .isInstanceOf(JedisDataException.class)
                        .hasMessage("ERR wrong number of arguments for 'hsetex' command")
        );

        assertThat(jedis.hgetAll(HASH)).isEmpty();
    }

    @TestTemplate
    public void hSetExWithMissingValue(Jedis jedis) {
        assertThat(jedis.hgetAll(HASH)).isEmpty();

        assertThat(List.of(
                new String[]{HASH, "FIELDS", "1", FIELD_1},
                new String[]{HASH, "FNX", "FIELDS", "1", FIELD_1},
                new String[]{HASH, "FIELDS", "2", FIELD_1, VALUE_1, FIELD_2}
        )).allSatisfy(args ->
                assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSETEX, args))
                        .isInstanceOf(JedisDataException.class)
                        .hasMessage("ERR wrong number of arguments for 'hsetex' command"));

        assertThat(jedis.hgetAll(HASH)).isEmpty();
    }

    @TestTemplate
    public void hSetExIgnoresFieldsArgCasing(Jedis jedis) {
        assertThat(jedis.hgetAll(HASH)).isEmpty();

        assertThat(jedis.sendCommand(Protocol.Command.HSETEX, HASH, "FiElDs", "2", FIELD_1, VALUE_1, FIELD_2, VALUE_2))
                .asInstanceOf(LONG)
                .isEqualTo(1);

        assertThat(jedis.hgetAll(HASH)).containsOnly(entry(FIELD_1, VALUE_1), entry(FIELD_2, VALUE_2));
    }

    @TestTemplate
    public void hSetExReturnsErrorWhenFieldsArgIsIncorrect(Jedis jedis) {
        assertThat(List.of(
                new String[]{HASH},
                new String[]{HASH, "FIELDS", "1", FIELD_1, VALUE_1, "FIELDS", "1", FIELD_2, VALUE_2}
        )).allSatisfy(args ->
                assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSETEX, HASH))
                        .isInstanceOf(JedisDataException.class)
                        .hasMessage("ERR wrong number of arguments for 'hsetex' command")
        );

        assertThat(List.of(
                new String[]{HASH, "NOTFIELDS", "2", FIELD_1, VALUE_1, FIELD_2, VALUE_2},
                new String[]{HASH, "FNX", "NOTFIELDS", "2", FIELD_1, VALUE_1, FIELD_2, VALUE_2},
                new String[]{HASH, "EX", "1", "NOTFIELDS", "2", FIELD_1, VALUE_1, FIELD_2, VALUE_2},
                new String[]{HASH, "FNX", "EX", "1", "NOTFIELDS", "2", FIELD_1, VALUE_1, FIELD_2, VALUE_2}
        )).allSatisfy((args) -> assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSETEX, args))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR unknown argument: NOTFIELDS"));

        assertThat(jedis.hgetAll(HASH)).isEmpty();
    }

    @TestTemplate
    public void hSetExReturnsErrorWhenNumFieldsIsIncorrect(Jedis jedis) {
        assertThat(List.of(
                new String[]{HASH},
                new String[]{HASH, "FIELDS"},
                new String[]{HASH, "FNX", "FIELDS"},
                new String[]{HASH, "FNX", "FIELDS"},
                new String[]{HASH, "EX", "1", "FIELDS"},
                new String[]{HASH, "FNX", "EX", "1", "FIELDS"},
                new String[]{HASH, "FNX", "EX", "1", "FIELDS", "0", FIELD_1},
                new String[]{HASH, "FIELDS", "1", FIELD_1, VALUE_1, FIELD_3, VALUE_3},
                new String[]{HASH, "FIELDS", "3", FIELD_1, VALUE_1, FIELD_3, VALUE_3}
        )).allSatisfy(args ->
                assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSETEX, args))
                        .isInstanceOf(JedisDataException.class)
                        .hasMessage("ERR wrong number of arguments for 'hsetex' command")
        );
        assertThat(List.of(
                new String[]{HASH, "FIELDS", "not a number", FIELD_1, VALUE_1, FIELD_3, VALUE_3},
                new String[]{HASH, "FIELDS", "-1", FIELD_1, VALUE_1, FIELD_3, VALUE_3},
                new String[]{HASH, "FIELDS", "0", FIELD_1, VALUE_1},
                new String[]{HASH, "FIELDS", "0", FIELD_1, VALUE_1, FIELD_3},
                new String[]{HASH, "FIELDS", "0", FIELD_1, VALUE_1, FIELD_3, VALUE_3}
        )).allSatisfy(args ->
                assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSETEX, args))
                        .isInstanceOf(JedisDataException.class)
                        .hasMessage("ERR invalid number of fields")
        );
        assertThat(jedis.hgetAll(HASH)).isEmpty();
    }

    @TestTemplate
    public void hSetExReturnsErrorWhenExpirationTimeArgumentIsIncorrect(Jedis jedis) {
        assertThat(List.of("EX", "PX", "EXAT", "PXAT")).allSatisfy(expiryOption -> {
            assertThat(List.of(
                    "not a number",
                    "1.2",
                    BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.valueOf(1)).toString()
            )).allSatisfy(expiryArg ->
                    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSETEX, HASH, expiryOption, expiryArg, "FIELDS", "1", FIELD_1, VALUE_1))
                            .isInstanceOf(JedisDataException.class)
                            .hasMessage("ERR value is not an integer or out of range")
            );
            assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSETEX, HASH, expiryOption, "-1", "FIELDS", "1", FIELD_1, VALUE_1))
                    .isInstanceOf(JedisDataException.class)
                    .hasMessage("ERR invalid expire time, must be >= 0");
            if (!expiryOption.equals("PXAT")) {
                assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSETEX, HASH, expiryOption, Long.toString(Long.MAX_VALUE), "FIELDS", "1", FIELD_1, VALUE_1))
                        .isInstanceOf(JedisDataException.class)
                        .hasMessage("ERR invalid expire time in 'hsetex' command");
            }
        });
        assertThat(jedis.hgetAll(HASH)).isEmpty();
    }

    @TestTemplate
    public void hSetExAcceptsPreFieldsOptionsInAnyOrder(Jedis jedis) {
        assertThat(List.of(
                new String[]{HASH, "EX", "1", "FNX", "FIELDS", "1", FIELD_1, VALUE_1},
                new String[]{HASH, "FNX", "EX", "1", "FIELDS", "1", FIELD_1, VALUE_1}
        )).allSatisfy(args -> {
            jedis.flushAll();

            jedis.sendCommand(Protocol.Command.HSETEX, args);

            assertThat(jedis.hgetAll(HASH)).containsOnly(entry(FIELD_1, VALUE_1));
        });
    }

    @TestTemplate
    public void hSetExRejectsConflictingFnxAndFxx(Jedis jedis) {
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSETEX, HASH, "FXX", "FNX", "FIELDS", "1", FIELD_1, VALUE_1))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Only one of FXX or FNX arguments can be specified");

        assertThat(jedis.hgetAll(HASH)).isEmpty();
    }

    @TestTemplate
    public void hSetExRejectsConflictingExpiryOptions(Jedis jedis) {
        List<String[]> options = List.of(
                new String[]{"EX", "100"},
                new String[]{"PX", "100"},
                new String[]{"EXAT", "100"},
                new String[]{"PXAT", "100"},
                new String[]{"KEEPTTL"}
        );
        assertThat(options).allSatisfy(option1 ->
                assertThat(options).allSatisfy(option2 -> {
                            List<String> args = new ArrayList<>();
                            args.add(HASH);
                            args.addAll(List.of(option1));
                            args.addAll(List.of(option2));
                            args.addAll(List.of("FIELDS", "1", FIELD_1, VALUE_1));
                            assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSETEX, args.toArray(new String[0])))
                                    .isInstanceOf(JedisDataException.class)
                                    .hasMessage("ERR Only one of EX, PX, EXAT, PXAT or KEEPTTL arguments can be specified");
                        }
                )
        );

        assertThat(jedis.hgetAll(HASH)).isEmpty();
    }

    @TestTemplate
    public void handlesSettingFieldOnLazilyExpiredFieldAndHash(Jedis jedis) {
        assertThat(List.of(
                new String[]{HASH, "EX", "0", "FIELDS", "1", FIELD_1, VALUE_1},
                new String[]{HASH, "PX", "0", "FIELDS", "1", FIELD_1, VALUE_1},
                new String[]{HASH, "EXAT", "100", "FIELDS", "1", FIELD_1, VALUE_1},
                new String[]{HASH, "PXAT", "100", "FIELDS", "1", FIELD_1, VALUE_1}
        )).allSatisfy(args -> {
            jedis.flushAll();
            jedis.hsetex(HASH, hSetExParams().exAt(100), FIELD_1, VALUE_1);

            jedis.sendCommand(Protocol.Command.HSETEX, args);

            assertThat(jedis.hgetAll(HASH)).isEmpty();
        });
    }

    @TestTemplate
    public void hSetExPublishesKeyspaceNotifications(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        jedis.hset(HASH, FIELD_2, VALUE_2);
        jedis.hexpire(HASH, 100, FIELD_2);

        try (NotificationCollector events = collectorFor(jedis, hostAndPort, "KEgh")) {
            assertThat(jedis.hsetex(HASH, hSetExParams(), FIELD_1, VALUE_1)).isEqualTo(1);
            assertThat(events.next(2)).containsExactly(
                    "__keyspace@0__:" + HASH + " -> hset",
                    "__keyevent@0__:hset -> " + HASH);

            // Putting the same value into the same field still triggers a notification.
            assertThat(jedis.hsetex(HASH, hSetExParams(), FIELD_1, VALUE_1)).isEqualTo(1);
            assertThat(events.next(2)).containsExactly(
                    "__keyspace@0__:" + HASH + " -> hset",
                    "__keyevent@0__:hset -> " + HASH);

            assertThat(jedis.hsetex(HASH, hSetExParams().fnx(), FIELD_1, VALUE_1)).isEqualTo(0);
            assertThat(jedis.hsetex(HASH, hSetExParams(), FIELD_3, VALUE_3)).isEqualTo(1);
            assertThat(events.next(2)).containsExactly(
                    "__keyspace@0__:" + HASH + " -> hset",
                    "__keyevent@0__:hset -> " + HASH);

            assertThat(jedis.httl(HASH, FIELD_2)).singleElement().asInstanceOf(LONG).isGreaterThan(0);
            assertThat(jedis.hsetex(HASH, hSetExParams(), FIELD_2, VALUE_2)).isEqualTo(1);
            // hpersist is not generated, even though the field's TTL was removed.
            assertThat(events.next(2)).containsExactly(
                    "__keyspace@0__:" + HASH + " -> hset",
                    "__keyevent@0__:hset -> " + HASH);
            assertThat(jedis.httl(HASH, FIELD_2)).singleElement().asInstanceOf(LONG).isEqualTo(-1);

            assertThat(jedis.hsetex(HASH, hSetExParams(), Map.of(FIELD_1, VALUE_1, FIELD_4, VALUE_4))).isEqualTo(1);
            assertThat(events.next(2)).containsExactly(
                    "__keyspace@0__:" + HASH + " -> hset",
                    "__keyevent@0__:hset -> " + HASH);

            events.assertNoFurtherNotifications();
        }
    }
}
