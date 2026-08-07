package com.github.fppt.jedismock.comparisontests.strings;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.GetExParams;
import redis.clients.jedis.params.SetParams;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * GETEX: a GET that may also change the key's expiration.
 * <p>
 * The option is parsed before anything else, so a syntax error is reported
 * even for a missing key or a key of the wrong type; conversely an out-of-range
 * expiration is only reported once the key has been found to exist and to be a
 * string. Nothing but the expiration is touched — the value is never written,
 * and without an option the TTL is left exactly as it was.
 */
@ExtendWith(ComparisonBase.class)
public class GetExTest {

    private static final String INVALID_EXPIRE = "ERR invalid expire time in 'getex' command";
    private static final String NOT_AN_INTEGER = "ERR value is not an integer or out of range";
    private static final String SYNTAX_ERROR = "ERR syntax error";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    /** Sends GETEX verbatim, for the cases Jedis's typed API cannot express. */
    private static Object getex(Jedis jedis, String... args) {
        return jedis.sendCommand(Protocol.Command.GETEX, args);
    }

    private static String getexString(Jedis jedis, String... args) {
        Object reply = getex(jedis, args);
        //Jedis encodes the request as UTF-8, so decode the reply the same way
        //rather than with whatever the platform default happens to be
        return reply == null ? null : new String((byte[]) reply, StandardCharsets.UTF_8);
    }

    @TestTemplate
    public void documentationExample(Jedis jedis) {
        assertThat(jedis.set("mykey", "Hello")).isEqualTo("OK");
        assertThat(jedis.getEx("mykey", GetExParams.getExParams())).isEqualTo("Hello");
        assertThat(jedis.ttl("mykey")).isEqualTo(-1L);
        assertThat(jedis.getEx("mykey", GetExParams.getExParams().ex(60))).isEqualTo("Hello");
        assertThat(jedis.ttl("mykey")).isEqualTo(60L);
    }

    @TestTemplate
    public void withoutAnOptionTheTtlIsLeftAlone(Jedis jedis) {
        jedis.set("foo", "bar", SetParams.setParams().ex(100));
        assertThat(jedis.getEx("foo", GetExParams.getExParams())).isEqualTo("bar");
        assertThat(jedis.ttl("foo")).isBetween(90L, 100L);
    }

    @TestTemplate
    public void theValueIsNeverModified(Jedis jedis) {
        jedis.set("foo", "bar");
        jedis.getEx("foo", GetExParams.getExParams().ex(100));
        assertThat(jedis.get("foo")).isEqualTo("bar");
    }

    @TestTemplate
    public void missingKeyReturnsNullAndIsNotCreated(Jedis jedis) {
        assertThat(jedis.getEx("nosuchkey", GetExParams.getExParams())).isNull();
        assertThat(jedis.getEx("nosuchkey", GetExParams.getExParams().ex(100))).isNull();
        assertThat(jedis.getEx("nosuchkey", GetExParams.getExParams().persist())).isNull();
        assertThat(jedis.exists("nosuchkey")).isFalse();
    }

    @TestTemplate
    public void pxSetsAMillisecondTtl(Jedis jedis) {
        jedis.set("foo", "bar");
        assertThat(jedis.getEx("foo", GetExParams.getExParams().px(100_000))).isEqualTo("bar");
        assertThat(jedis.pttl("foo")).isBetween(90_000L, 100_000L);
    }

    @TestTemplate
    public void exAtSetsAnAbsoluteTtl(Jedis jedis) {
        jedis.set("foo", "bar");
        long deadline = System.currentTimeMillis() / 1000 + 100;
        assertThat(jedis.getEx("foo", GetExParams.getExParams().exAt(deadline))).isEqualTo("bar");
        assertThat(jedis.ttl("foo")).isBetween(90L, 100L);
        assertThat(jedis.expireTime("foo")).isEqualTo(deadline);
    }

    @TestTemplate
    public void pxAtSetsAnAbsoluteTtl(Jedis jedis) {
        jedis.set("foo", "bar");
        long deadline = System.currentTimeMillis() + 100_000;
        assertThat(jedis.getEx("foo", GetExParams.getExParams().pxAt(deadline))).isEqualTo("bar");
        assertThat(jedis.pttl("foo")).isBetween(90_000L, 100_000L);
        assertThat(jedis.pexpireTime("foo")).isEqualTo(deadline);
    }

    @TestTemplate
    public void anExistingTtlIsReplacedRatherThanShortened(Jedis jedis) {
        jedis.set("foo", "bar", SetParams.setParams().ex(10));
        jedis.getEx("foo", GetExParams.getExParams().ex(1000));
        assertThat(jedis.ttl("foo")).isBetween(990L, 1000L);
        jedis.getEx("foo", GetExParams.getExParams().ex(10));
        assertThat(jedis.ttl("foo")).isBetween(1L, 10L);
    }

    //Equivalent of the commented-out expire.tcl test
    //{GETEX use of PERSIST option should remove TTL}
    @TestTemplate
    public void persistRemovesTtl(Jedis jedis) {
        jedis.set("foo", "bar", SetParams.setParams().ex(100));
        assertThat(jedis.getEx("foo", GetExParams.getExParams().persist())).isEqualTo("bar");
        assertThat(jedis.ttl("foo")).isEqualTo(-1L);
    }

    @TestTemplate
    public void persistOnAKeyWithoutTtlIsHarmless(Jedis jedis) {
        jedis.set("foo", "bar");
        assertThat(jedis.getEx("foo", GetExParams.getExParams().persist())).isEqualTo("bar");
        assertThat(jedis.ttl("foo")).isEqualTo(-1L);
        assertThat(jedis.exists("foo")).isTrue();
    }

    @TestTemplate
    public void exAtInThePastDeletesTheKey(Jedis jedis) {
        jedis.set("foo", "bar");
        //The value is still returned, and only then is the key dropped
        assertThat(jedis.getEx("foo", GetExParams.getExParams().exAt(1))).isEqualTo("bar");
        assertThat(jedis.exists("foo")).isFalse();
    }

    @TestTemplate
    public void pxAtInThePastDeletesTheKey(Jedis jedis) {
        jedis.set("foo", "bar");
        assertThat(jedis.getEx("foo", GetExParams.getExParams().pxAt(1))).isEqualTo("bar");
        assertThat(jedis.exists("foo")).isFalse();
    }

    //Equivalent of the commented-out expire.tcl test
    //{GETEX with big integer should report an error}
    @TestTemplate
    public void withBigIntegerShouldReportAnError(Jedis jedis) {
        jedis.set("foo", "bar");
        assertThatThrownBy(() -> getex(jedis, "foo", "EX", "10000000000000000"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage(INVALID_EXPIRE);
        assertThat(jedis.ttl("foo")).isEqualTo(-1L);
    }

    //Equivalent of the commented-out expire.tcl test
    //{GETEX with smallest integer should report an error}
    @TestTemplate
    public void withSmallestIntegerShouldReportAnError(Jedis jedis) {
        jedis.set("foo", "bar");
        assertThatThrownBy(() -> getex(jedis, "foo", "EX", "-9999999999999999"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage(INVALID_EXPIRE);
        assertThat(jedis.ttl("foo")).isEqualTo(-1L);
    }

    @TestTemplate
    public void aNonPositiveExpirationIsRejected(Jedis jedis) {
        jedis.set("foo", "bar", SetParams.setParams().ex(100));
        //Zero is out of range for every unit, absolute ones included
        for (String[] args : new String[][]{
                {"foo", "EX", "0"}, {"foo", "PX", "0"},
                {"foo", "EXAT", "0"}, {"foo", "PXAT", "0"},
                {"foo", "EX", "-1"}, {"foo", "PX", "-1"},
                {"foo", "EXAT", "-1"}, {"foo", "PXAT", "-1"}}) {
            assertThatThrownBy(() -> getex(jedis, args))
                    .as("GETEX %s %s %s", args[0], args[1], args[2])
                    .isInstanceOf(JedisDataException.class)
                    .hasMessage(INVALID_EXPIRE);
        }
        assertThat(jedis.ttl("foo")).isBetween(90L, 100L);
    }

    @TestTemplate
    public void secondsAreRejectedOnceTheyOverflowMilliseconds(Jedis jedis) {
        jedis.set("foo", "bar");
        //Both EX and EXAT are multiplied by 1000, so both overflow at LLONG_MAX/1000
        assertThatThrownBy(() -> getex(jedis, "foo", "EXAT", "9999999999999999"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage(INVALID_EXPIRE);
        assertThatThrownBy(() -> getex(jedis, "foo", "EX", "9223372036854776"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage(INVALID_EXPIRE);
        //A relative expiration is added to the current time, an absolute one is not
        assertThatThrownBy(() -> getex(jedis, "foo", "PX", String.valueOf(Long.MAX_VALUE)))
                .isInstanceOf(JedisDataException.class)
                .hasMessage(INVALID_EXPIRE);
        assertThat(getexString(jedis, "foo", "PXAT", String.valueOf(Long.MAX_VALUE))).isEqualTo("bar");
        assertThat(jedis.pexpireTime("foo")).isEqualTo(Long.MAX_VALUE);
    }

    @TestTemplate
    public void aNonNumericExpirationIsRejected(Jedis jedis) {
        jedis.set("foo", "bar");
        assertThatThrownBy(() -> getex(jedis, "foo", "EX", "abc"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage(NOT_AN_INTEGER);
        assertThatThrownBy(() -> getex(jedis, "foo", "PXAT", "1.5"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage(NOT_AN_INTEGER);
        assertThat(jedis.ttl("foo")).isEqualTo(-1L);
    }

    @TestTemplate
    public void unknownIncompleteAndConflictingOptionsAreSyntaxErrors(Jedis jedis) {
        jedis.set("foo", "bar");
        for (String[] args : new String[][]{
                {"foo", "BADOPT"},
                {"foo", "XX"},
                {"foo", "KEEPTTL"},
                {"foo", "EX"},
                {"foo", "EX", "100", "100"},
                {"foo", "EX", "100", "PX", "100"},
                {"foo", "EX", "100", "PERSIST"},
                {"foo", "PERSIST", "EX", "100"},
                {"foo", "EXAT", "1", "PXAT", "1"}}) {
            assertThatThrownBy(() -> getex(jedis, args))
                    .as("GETEX %s", String.join(" ", args))
                    .isInstanceOf(JedisDataException.class)
                    .hasMessage(SYNTAX_ERROR);
        }
        assertThat(jedis.ttl("foo")).isEqualTo(-1L);
    }

    @TestTemplate
    public void aRepeatedOptionKeepsTheLastValue(Jedis jedis) {
        jedis.set("foo", "bar");
        //Only *different* options conflict; the same one twice is accepted
        assertThat(getexString(jedis, "foo", "EX", "100", "EX", "300")).isEqualTo("bar");
        assertThat(jedis.ttl("foo")).isBetween(290L, 300L);
        getex(jedis, "foo", "PERSIST", "PERSIST");
        assertThat(jedis.ttl("foo")).isEqualTo(-1L);
    }

    @TestTemplate
    public void optionsAreCaseInsensitive(Jedis jedis) {
        jedis.set("foo", "bar");
        getex(jedis, "foo", "ex", "100");
        assertThat(jedis.ttl("foo")).isBetween(90L, 100L);
        getex(jedis, "foo", "PeRsIsT");
        assertThat(jedis.ttl("foo")).isEqualTo(-1L);
    }

    @TestTemplate
    public void withoutAKeyItIsAnArityError(Jedis jedis) {
        assertThatThrownBy(() -> getex(jedis))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR wrong number of arguments for 'getex' command");
    }

    @TestTemplate
    public void aKeyOfTheWrongTypeIsReported(Jedis jedis) {
        jedis.hset("h", "f", "v");
        assertThatThrownBy(() -> jedis.getEx("h", GetExParams.getExParams()))
                .isInstanceOf(JedisDataException.class)
                .hasMessageStartingWith("WRONGTYPE");
        assertThatThrownBy(() -> jedis.getEx("h", GetExParams.getExParams().ex(100)))
                .isInstanceOf(JedisDataException.class)
                .hasMessageStartingWith("WRONGTYPE");
        assertThat(jedis.hget("h", "f")).isEqualTo("v");
        assertThat(jedis.ttl("h")).isEqualTo(-1L);
    }

    @TestTemplate
    public void theOptionIsParsedBeforeTheKeyIsEvenLookedUp(Jedis jedis) {
        jedis.rpush("l", "a");
        //A syntax error outranks both a missing key and a wrong type
        assertThatThrownBy(() -> getex(jedis, "nosuchkey", "BADOPT"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage(SYNTAX_ERROR);
        assertThatThrownBy(() -> getex(jedis, "l", "BADOPT"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage(SYNTAX_ERROR);
    }

    @TestTemplate
    public void anOutOfRangeExpirationIsOnlyReportedForAnExistingStringKey(Jedis jedis) {
        jedis.rpush("l", "a");
        //A missing key wins: nil, not an error
        assertThat(getex(jedis, "nosuchkey", "EX", "0")).isNull();
        assertThat(getex(jedis, "nosuchkey", "EX", "abc")).isNull();
        //A wrong type wins too
        assertThatThrownBy(() -> getex(jedis, "l", "EX", "0"))
                .isInstanceOf(JedisDataException.class)
                .hasMessageStartingWith("WRONGTYPE");
    }

    @TestTemplate
    public void expiredKeysAreTreatedAsMissing(Jedis jedis) {
        jedis.set("foo", "bar", SetParams.setParams().px(1));
        //The poll must not carry an option: an expiring one would reinstate the
        //TTL on any round that still saw the key, and it would never expire
        Awaitility.await().until(() -> jedis.getEx("foo", GetExParams.getExParams()) == null);
        //Nor can the expiring form resurrect what has already gone
        assertThat(jedis.getEx("foo", GetExParams.getExParams().ex(100))).isNull();
        assertThat(jedis.exists("foo")).isFalse();
    }
}
