package com.github.fppt.jedismock.comparisontests.strings;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.params.SetParams;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code SET key value GET}: the assignment replies with the value the key
 * held beforehand instead of OK, or nil when it held nothing.
 * <p>
 * The reply describes the old value even when the write itself is declined, so
 * {@code SET k v NX GET} on an existing key reports that value and changes
 * nothing. Because the option has to read the key, it is also the only way
 * SET can answer WRONGTYPE — and it does so only after the option list and the
 * expiration have both been found sound, which puts it fifth in the order of
 * failures: arity, syntax, non-integer expiration, out-of-range expiration,
 * then WRONGTYPE.
 */
@ExtendWith(ComparisonBase.class)
public class TestSetGet {

    private static final String WRONG_TYPE = "WRONGTYPE Operation against a key holding the wrong kind of value";
    private static final String SYNTAX_ERROR = "ERR syntax error";
    private static final String NOT_AN_INTEGER = "ERR value is not an integer or out of range";
    private static final String INVALID_EXPIRE = "ERR invalid expire time in 'set' command";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
    }

    private static Object set(Jedis jedis, String... args) {
        return jedis.sendCommand(Protocol.Command.SET, args);
    }

    private static String setReply(Jedis jedis, String... args) {
        Object reply = set(jedis, args);
        //Jedis encodes the request as UTF-8, so decode the reply the same way
        //rather than with whatever the platform default happens to be
        return reply == null ? null : new String((byte[]) reply, StandardCharsets.UTF_8);
    }

    @TestTemplate
    public void returnsThePreviousValueAndStillWrites(Jedis jedis) {
        jedis.set("k", "old");
        assertThat(jedis.setGet("k", "new")).isEqualTo("old");
        assertThat(jedis.get("k")).isEqualTo("new");
    }

    @TestTemplate
    public void returnsNilForAMissingKeyAndStillWrites(Jedis jedis) {
        assertThat(jedis.setGet("k", "new")).isNull();
        assertThat(jedis.get("k")).isEqualTo("new");
    }

    @TestTemplate
    public void distinguishesAnEmptyPreviousValueFromAMissingKey(Jedis jedis) {
        jedis.set("empty", "");
        assertThat(jedis.setGet("empty", "v")).isEqualTo("");
        assertThat(jedis.setGet("absent", "v")).isNull();
    }

    @TestTemplate
    public void nxDeclinesTheWriteButStillReportsThePreviousValue(Jedis jedis) {
        jedis.set("k", "old");
        assertThat(jedis.setGet("k", "new", SetParams.setParams().nx())).isEqualTo("old");
        //Reported, but deliberately not written
        assertThat(jedis.get("k")).isEqualTo("old");

        assertThat(jedis.setGet("fresh", "new", SetParams.setParams().nx())).isNull();
        assertThat(jedis.get("fresh")).isEqualTo("new");
    }

    @TestTemplate
    public void xxReportsThePreviousValueOrNilWhenThereIsNone(Jedis jedis) {
        assertThat(jedis.setGet("absent", "new", SetParams.setParams().xx())).isNull();
        assertThat(jedis.exists("absent")).isFalse();

        jedis.set("k", "old");
        assertThat(jedis.setGet("k", "new", SetParams.setParams().xx())).isEqualTo("old");
        assertThat(jedis.get("k")).isEqualTo("new");
    }

    @TestTemplate
    public void appliesTheExpirationAsUsual(Jedis jedis) {
        jedis.set("k", "old");
        assertThat(jedis.setGet("k", "new", SetParams.setParams().ex(100L))).isEqualTo("old");
        assertThat(jedis.ttl("k")).isEqualTo(100L);
    }

    @TestTemplate
    public void keepTtlKeepsTheExpirationAndItsAbsenceClearsIt(Jedis jedis) {
        jedis.set("kept", "old", SetParams.setParams().ex(50L));
        assertThat(jedis.setGet("kept", "new", SetParams.setParams().keepTtl())).isEqualTo("old");
        assertThat(jedis.ttl("kept")).isBetween(1L, 50L);

        jedis.set("cleared", "old", SetParams.setParams().ex(50L));
        assertThat(jedis.setGet("cleared", "new")).isEqualTo("old");
        assertThat(jedis.ttl("cleared")).isEqualTo(-1L);
    }

    @TestTemplate
    public void reportsWrongTypeAndWritesNothing(Jedis jedis) {
        jedis.rpush("mylist", "a");
        assertThatThrownBy(() -> jedis.setGet("mylist", "v")).hasMessage(WRONG_TYPE);
        //NX and XX do not get to decide: the read happens first either way
        assertThatThrownBy(() -> jedis.setGet("mylist", "v", SetParams.setParams().nx()))
                .hasMessage(WRONG_TYPE);
        assertThatThrownBy(() -> jedis.setGet("mylist", "v", SetParams.setParams().xx()))
                .hasMessage(WRONG_TYPE);
        assertThat(jedis.type("mylist")).isEqualTo("list");

        jedis.hset("myhash", "f", "v");
        assertThatThrownBy(() -> jedis.setGet("myhash", "v")).hasMessage(WRONG_TYPE);
        jedis.zadd("myzset", 1, "a");
        assertThatThrownBy(() -> jedis.setGet("myzset", "v")).hasMessage(WRONG_TYPE);
    }

    @TestTemplate
    public void withoutGetTheSameWritesReplaceTheKeyHappily(Jedis jedis) {
        //GET is the only reason SET ever reports WRONGTYPE
        jedis.rpush("mylist", "a");
        assertThat(jedis.set("mylist", "v")).isEqualTo("OK");
        assertThat(jedis.get("mylist")).isEqualTo("v");
    }

    @TestTemplate
    public void expirationFailuresOutrankWrongType(Jedis jedis) {
        jedis.rpush("mylist", "a");
        //The expiration is validated before GET reads anything
        assertThatThrownBy(() -> set(jedis, "mylist", "v", "GET", "EX", "0"))
                .hasMessage(INVALID_EXPIRE);
        assertThatThrownBy(() -> set(jedis, "mylist", "v", "GET", "EX", "notanumber"))
                .hasMessage(NOT_AN_INTEGER);
        assertThatThrownBy(() -> set(jedis, "mylist", "v", "GET", "badoption"))
                .hasMessage(SYNTAX_ERROR);
        //Written the other way round the outcome is the same
        assertThatThrownBy(() -> set(jedis, "mylist", "v", "EX", "0", "GET"))
                .hasMessage(INVALID_EXPIRE);
        //A sound expiration lets the WRONGTYPE through
        assertThatThrownBy(() -> set(jedis, "mylist", "v", "GET", "EX", "100"))
                .hasMessage(WRONG_TYPE);
        assertThat(jedis.type("mylist")).isEqualTo("list");
    }

    @TestTemplate
    public void repeatingGetIsAccepted(Jedis jedis) {
        jedis.set("k", "old");
        assertThat(setReply(jedis, "k", "new", "GET", "GET")).isEqualTo("old");
        assertThat(jedis.get("k")).isEqualTo("new");
    }

    @TestTemplate
    public void isBinarySafe(Jedis jedis) {
        byte[] key = "bin".getBytes(StandardCharsets.UTF_8);
        byte[] old = {'a', 0, 'b', (byte) 0xff};
        byte[] fresh = {(byte) 0xfe, 1, 'c'};
        jedis.set(key, old);
        assertThat(jedis.setGet(key, fresh)).isEqualTo(old);
        assertThat(jedis.get(key)).isEqualTo(fresh);
    }

    @TestTemplate
    public void anExpiredPreviousValueReadsAsAbsent(Jedis jedis) throws InterruptedException {
        jedis.set("k", "old", SetParams.setParams().px(50L));
        Thread.sleep(150);
        assertThat(jedis.setGet("k", "new")).isNull();
        assertThat(jedis.get("k")).isEqualTo("new");
    }
}
