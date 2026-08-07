package com.github.fppt.jedismock.comparisontests.strings;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * GETRANGE and SUBSTR are two names for one command: SUBSTR is the original
 * (Redis 1.0), deprecated in 2.0 and renamed GETRANGE in 2.4. The server
 * dispatches both to the same implementation, so every assertion here is made
 * against both names — the only observable difference is which name appears in
 * the arity error.
 * <p>
 * Both offsets are inclusive and may be negative (counting back from the end).
 * They are parsed before the key is looked up, so a malformed offset is
 * reported ahead of WRONGTYPE or a missing key. Clamping is the subtle part:
 * an end that is still negative after adding the length collapses to 0 rather
 * than to "empty", so {@code GETRANGE key 0 -100} returns the first character.
 */
@ExtendWith(ComparisonBase.class)
public class TestGetRange {

    private static final String NOT_AN_INTEGER = "ERR value is not an integer or out of range";
    private static final String WRONG_TYPE = "WRONGTYPE Operation against a key holding the wrong kind of value";

    private static final Protocol.Command[] BOTH_NAMES = {
            Protocol.Command.GETRANGE, Protocol.Command.SUBSTR
    };

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    private static String range(Jedis jedis, Protocol.Command command, String key, String start, String end) {
        Object reply = jedis.sendCommand(command, key, start, end);
        //Jedis encodes the request as UTF-8, so decode the reply the same way
        //rather than with whatever the platform default happens to be
        return reply == null ? null : new String((byte[]) reply, StandardCharsets.UTF_8);
    }

    /** Asserts the reply under both names, since they must not diverge. */
    private static void assertRange(Jedis jedis, String key, String start, String end, String expected) {
        for (Protocol.Command command : BOTH_NAMES) {
            assertThat(range(jedis, command, key, start, end))
                    .describedAs("%s %s %s %s", command, key, start, end)
                    .isEqualTo(expected);
        }
    }

    private static void assertRangeFails(Jedis jedis, String message, String... args) {
        for (Protocol.Command command : BOTH_NAMES) {
            assertThatThrownBy(() -> jedis.sendCommand(command, args))
                    .describedAs("%s %s", command, String.join(" ", args))
                    .hasMessage(message);
        }
    }

    @TestTemplate
    public void documentationExample(Jedis jedis) {
        jedis.set("mykey", "This is a string");
        assertRange(jedis, "mykey", "0", "3", "This");
        assertRange(jedis, "mykey", "-3", "-1", "ing");
        assertRange(jedis, "mykey", "0", "-1", "This is a string");
        assertRange(jedis, "mykey", "10", "100", "string");
    }

    @TestTemplate
    public void typedApiAgreesOnBothNames(Jedis jedis) {
        jedis.set("mykey", "This is a string");
        assertThat(jedis.getrange("mykey", 0, 3)).isEqualTo("This");
        assertThat(jedis.substr("mykey", 0, 3)).isEqualTo("This");
        assertThat(jedis.getrange("mykey", -3, -1)).isEqualTo("ing");
        assertThat(jedis.substr("mykey", -3, -1)).isEqualTo("ing");
    }

    @TestTemplate
    public void againstNonExistingKeyReturnsEmptyString(Jedis jedis) {
        assertRange(jedis, "nosuchkey", "0", "-1", "");
        assertRange(jedis, "nosuchkey", "0", "100", "");
        //An empty bulk string, not a nil — unlike GET
        assertThat(jedis.getrange("nosuchkey", 0, -1)).isEqualTo("");
        assertThat(jedis.substr("nosuchkey", 0, -1)).isEqualTo("");
    }

    @TestTemplate
    public void againstStringValue(Jedis jedis) {
        jedis.set("mykey", "Hello World");
        assertRange(jedis, "mykey", "0", "3", "Hell");
        assertRange(jedis, "mykey", "0", "-1", "Hello World");
        assertRange(jedis, "mykey", "-4", "-1", "orld");
        assertRange(jedis, "mykey", "5", "3", "");
        assertRange(jedis, "mykey", "5", "5000", " World");
        assertRange(jedis, "mykey", "-5000", "10000", "Hello World");
    }

    @TestTemplate
    public void againstIntegerEncodedValue(Jedis jedis) {
        jedis.set("mykey", "1234");
        assertRange(jedis, "mykey", "0", "2", "123");
        assertRange(jedis, "mykey", "0", "-1", "1234");
        assertRange(jedis, "mykey", "-3", "-1", "234");
        assertRange(jedis, "mykey", "5", "3", "");
        assertRange(jedis, "mykey", "3", "5000", "4");
        assertRange(jedis, "mykey", "-5000", "10000", "1234");
    }

    @TestTemplate
    public void numericValuesAreSlicedAsTheirDecimalText(Jedis jedis) {
        //Redis stores small integers in an 'int' encoding rather than as bytes,
        //but GETRANGE materialises the decimal text first, so offsets count
        //characters — a leading minus sign included
        jedis.set("negative", "-1234");
        assertRange(jedis, "negative", "0", "0", "-");
        assertRange(jedis, "negative", "0", "2", "-12");
        assertRange(jedis, "negative", "-2", "-1", "34");

        //No normalisation of any kind: the text is sliced exactly as written
        jedis.set("padded", "007");
        assertRange(jedis, "padded", "0", "0", "0");
        assertRange(jedis, "padded", "0", "-1", "007");
        jedis.set("float", "3.14");
        assertRange(jedis, "float", "0", "2", "3.1");
        jedis.set("exponent", "1e3");
        assertRange(jedis, "exponent", "0", "-1", "1e3");

        jedis.set("big", "9223372036854775807");
        assertRange(jedis, "big", "0", "2", "922");
        assertRange(jedis, "big", "-2", "-1", "07");
    }

    @TestTemplate
    public void againstValueCreatedNumerically(Jedis jedis) {
        //A key that was never written as a string still slices as its text
        jedis.incrBy("counter", 42);
        assertRange(jedis, "counter", "0", "-1", "42");
        assertRange(jedis, "counter", "0", "0", "4");
        assertRange(jedis, "counter", "-1", "-1", "2");
    }

    @TestTemplate
    public void againstEmptyStringValue(Jedis jedis) {
        jedis.set("mykey", "");
        assertRange(jedis, "mykey", "0", "-1", "");
        assertRange(jedis, "mykey", "0", "0", "");
        assertRange(jedis, "mykey", "-1", "-1", "");
    }

    @TestTemplate
    public void singleCharacterRanges(Jedis jedis) {
        jedis.set("mykey", "This is a string");
        assertRange(jedis, "mykey", "0", "0", "T");
        assertRange(jedis, "mykey", "-1", "-1", "g");
        assertRange(jedis, "mykey", "15", "15", "g");
        //One past the last index
        assertRange(jedis, "mykey", "16", "16", "");
    }

    @TestTemplate
    public void startAfterEndIsEmpty(Jedis jedis) {
        jedis.set("mykey", "This is a string");
        assertRange(jedis, "mykey", "5", "2", "");
        assertRange(jedis, "mykey", "-1", "0", "");
        //-5 resolves to 11, which is past the end at 10
        assertRange(jedis, "mykey", "-5", "10", "");
        assertRange(jedis, "mykey", "-5", "-10", "");
        //-3 resolves to 13, -20 clamps to 0
        assertRange(jedis, "mykey", "-3", "-20", "");
    }

    @TestTemplate
    public void negativeEndClampsToZeroNotToEmpty(Jedis jedis) {
        jedis.set("mykey", "This is a string");
        //An end still negative after adding the length becomes 0, so these
        //return the first character rather than nothing
        assertRange(jedis, "mykey", "0", "-100", "T");
        assertRange(jedis, "mykey", "-20", "-18", "T");
        assertRange(jedis, "mykey", "-20", "-17", "T");
        assertRange(jedis, "mykey", "-100", "-100", "T");
    }

    @TestTemplate
    public void hugeRanges(Jedis jedis) {
        jedis.set("foo", "bar");
        //Github issue #1844: an end past 32 bits must not wrap around
        assertRange(jedis, "foo", "0", "4294967297", "bar");
        assertRange(jedis, "foo", "-4294967297", "-1", "bar");
    }

    @TestTemplate
    public void longBoundaryOffsets(Jedis jedis) {
        jedis.set("mykey", "This is a string");
        assertRange(jedis, "mykey", "0", "9223372036854775807", "This is a string");
        assertRange(jedis, "mykey", "-9223372036854775808", "-1", "This is a string");
        assertRange(jedis, "mykey", "9223372036854775807", "-1", "");
    }

    @TestTemplate
    public void offsetsOutsideLongRangeAreRejected(Jedis jedis) {
        jedis.set("mykey", "This is a string");
        assertRangeFails(jedis, NOT_AN_INTEGER, "mykey", "0", "99999999999999999999");
        assertRangeFails(jedis, NOT_AN_INTEGER, "mykey", "-99999999999999999999", "-1");
    }

    @TestTemplate
    public void nonIntegerOffsetsAreRejected(Jedis jedis) {
        jedis.set("mykey", "This is a string");
        assertRangeFails(jedis, NOT_AN_INTEGER, "mykey", "a", "3");
        assertRangeFails(jedis, NOT_AN_INTEGER, "mykey", "0", "b");
        assertRangeFails(jedis, NOT_AN_INTEGER, "mykey", "1.5", "3");
        assertRangeFails(jedis, NOT_AN_INTEGER, "mykey", "", "3");
    }

    @TestTemplate
    public void againstKeyWithWrongType(Jedis jedis) {
        jedis.rpush("mylist", "a");
        assertRangeFails(jedis, WRONG_TYPE, "mylist", "0", "-1");
    }

    @TestTemplate
    public void offsetsAreParsedBeforeTheKeyIsLookedUp(Jedis jedis) {
        jedis.rpush("mylist", "a");
        //The bad offset wins over both WRONGTYPE and the missing key
        assertRangeFails(jedis, NOT_AN_INTEGER, "mylist", "a", "b");
        assertRangeFails(jedis, NOT_AN_INTEGER, "nosuchkey", "a", "b");
    }

    @TestTemplate
    public void wrongNumberOfArgumentsNamesTheCommandAsIssued(Jedis jedis) {
        for (Protocol.Command command : BOTH_NAMES) {
            String name = command.name().toLowerCase();
            String message = String.format("ERR wrong number of arguments for '%s' command", name);
            assertThatThrownBy(() -> jedis.sendCommand(command))
                    .hasMessage(message);
            assertThatThrownBy(() -> jedis.sendCommand(command, "mykey"))
                    .hasMessage(message);
            assertThatThrownBy(() -> jedis.sendCommand(command, "mykey", "0"))
                    .hasMessage(message);
            assertThatThrownBy(() -> jedis.sendCommand(command, "mykey", "0", "1", "2"))
                    .hasMessage(message);
        }
    }

    @TestTemplate
    public void isBinarySafe(Jedis jedis) {
        byte[] key = "bin".getBytes(StandardCharsets.UTF_8);
        byte[] value = {'a', 0, 'b', (byte) 0xff, 'c'};
        jedis.set(key, value);
        assertThat(jedis.getrange(key, 0, -1)).isEqualTo(value);
        assertThat(jedis.substr(key, 0, -1)).isEqualTo(value);
        assertThat(jedis.getrange(key, 1, 3)).isEqualTo(new byte[]{0, 'b', (byte) 0xff});
        assertThat(jedis.substr(key, 1, 3)).isEqualTo(new byte[]{0, 'b', (byte) 0xff});
        assertThat(jedis.getrange(key, -2, -1)).isEqualTo(new byte[]{(byte) 0xff, 'c'});
    }

    @TestTemplate
    public void isReadOnly(Jedis jedis) {
        jedis.setex("mykey", 100, "This is a string");
        assertRange(jedis, "mykey", "0", "3", "This");
        //Neither the value nor its expiration is touched
        assertThat(jedis.get("mykey")).isEqualTo("This is a string");
        assertThat(jedis.ttl("mykey")).isEqualTo(100L);
    }
}
