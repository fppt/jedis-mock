package com.github.fppt.jedismock.comparisontests.strings;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * How SET validates its option list, which is a single left-to-right pass:
 * an unknown token, an option that conflicts with one already seen, or an
 * expiration option with nothing following it all produce {@code ERR syntax
 * error}. Repeating the <em>same</em> option is fine and the last one wins.
 * <p>
 * The whole list is checked for syntax before the expiration value is looked
 * at, so a bad option beats a bad expiration however they are ordered — only
 * once the list parses does the value get converted (ERR value is not an
 * integer) and then range-checked (ERR invalid expire time).
 */
@ExtendWith(ComparisonBase.class)
public class TestSetOptionParsing {

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

    /** The reply decoded as text: "OK", or null when SET declines to write. */
    private static String setReply(Jedis jedis, String... args) {
        Object reply = set(jedis, args);
        //Jedis encodes the request as UTF-8, so decode the reply the same way
        //rather than with whatever the platform default happens to be
        return reply == null ? null : new String((byte[]) reply, StandardCharsets.UTF_8);
    }

    private static void assertSyntaxError(SoftAssertions softly, Jedis jedis, String... args) {
        softly.assertThatThrownBy(() -> set(jedis, args))
                .describedAs("SET %s", String.join(" ", (CharSequence[]) args))
                .hasMessage(SYNTAX_ERROR);
    }

    @TestTemplate
    public void unknownOptionIsASyntaxError(Jedis jedis) {
        //The case from the native suite: "Extended SET can detect syntax errors"
        assertThatThrownBy(() -> set(jedis, "foo", "bar", "non-existing-option"))
                .hasMessage(SYNTAX_ERROR);
        assertThat(jedis.exists("foo")).isFalse();
    }

    @TestTemplate
    public void conflictingOptionsAreASyntaxError(Jedis jedis) {
        SoftAssertions softly = new SoftAssertions();
        //NX and XX are mutually exclusive, in either order
        assertSyntaxError(softly, jedis, "foo", "bar", "nx", "xx");
        assertSyntaxError(softly, jedis, "foo", "bar", "xx", "nx");
        //So is any pair of different expiration options
        assertSyntaxError(softly, jedis, "foo", "bar", "ex", "10", "px", "10000");
        assertSyntaxError(softly, jedis, "foo", "bar", "ex", "10", "exat", "99999999999");
        assertSyntaxError(softly, jedis, "foo", "bar", "px", "10000", "pxat", "99999999999999");
        assertSyntaxError(softly, jedis, "foo", "bar", "exat", "99999999999", "pxat", "99999999999999");
        //KEEPTTL cannot be combined with an expiration, in either order
        assertSyntaxError(softly, jedis, "foo", "bar", "keepttl", "ex", "10");
        assertSyntaxError(softly, jedis, "foo", "bar", "ex", "10", "keepttl");
        assertSyntaxError(softly, jedis, "foo", "bar", "exat", "99999999999", "keepttl");
        assertSyntaxError(softly, jedis, "foo", "bar", "keepttl", "pxat", "99999999999999");
        softly.assertAll();
    }

    @TestTemplate
    public void repeatingTheSameOptionIsAllowedAndTheLastWins(Jedis jedis) {
        assertThat(setReply(jedis, "foo", "bar", "ex", "10", "ex", "200")).isEqualTo("OK");
        assertThat(jedis.ttl("foo")).isEqualTo(200L);
        assertThat(setReply(jedis, "foo", "bar", "keepttl", "keepttl")).isEqualTo("OK");
        assertThat(setReply(jedis, "foo", "bar", "xx", "xx")).isEqualTo("OK");
        //NX on an existing key declines the write rather than erroring
        assertThat(setReply(jedis, "foo", "bar", "nx", "nx")).isNull();
    }

    @TestTemplate
    public void expirationOptionRequiresAnArgument(Jedis jedis) {
        SoftAssertions softly = new SoftAssertions();
        assertSyntaxError(softly, jedis, "foo", "bar", "ex");
        assertSyntaxError(softly, jedis, "foo", "bar", "px");
        assertSyntaxError(softly, jedis, "foo", "bar", "exat");
        assertSyntaxError(softly, jedis, "foo", "bar", "pxat");
        //Even when other options precede it
        assertSyntaxError(softly, jedis, "foo", "bar", "nx", "ex");
        softly.assertAll();
    }

    @TestTemplate
    public void expirationOptionSwallowsWhateverFollowsIt(Jedis jedis) {
        //"nx" here is consumed as EX's argument, not treated as an option, so
        //this fails on the conversion rather than on the syntax
        assertThatThrownBy(() -> set(jedis, "foo", "bar", "ex", "nx"))
                .hasMessage(NOT_AN_INTEGER);
    }

    @TestTemplate
    public void optionsAreCaseInsensitive(Jedis jedis) {
        assertThat(setReply(jedis, "foo", "bar", "Ex", "10")).isEqualTo("OK");
        assertThat(jedis.ttl("foo")).isEqualTo(10L);
        assertThat(setReply(jedis, "foo", "bar", "KeepTTL")).isEqualTo("OK");
        assertThat(setReply(jedis, "foo2", "bar", "NX")).isEqualTo("OK");
        assertThat(setReply(jedis, "foo3", "bar", "pXaT", "99999999999999")).isEqualTo("OK");
    }

    @TestTemplate
    public void syntaxErrorIsReportedBeforeTheExpirationIsExamined(Jedis jedis) {
        SoftAssertions softly = new SoftAssertions();
        //Each of these has both a bad option and an unusable expiration; the
        //syntax error wins regardless of which comes first
        assertSyntaxError(softly, jedis, "foo", "bar", "ex", "0", "badoption");
        assertSyntaxError(softly, jedis, "foo", "bar", "badoption", "ex", "0");
        assertSyntaxError(softly, jedis, "foo", "bar", "ex", "notanumber", "badoption");
        softly.assertAll();
    }

    @TestTemplate
    public void nonIntegerExpirationIsReportedBeforeItsRange(Jedis jedis) {
        SoftAssertions softly = new SoftAssertions();
        softly.assertThatThrownBy(() -> set(jedis, "foo", "bar", "ex", "notanumber"))
                .hasMessage(NOT_AN_INTEGER);
        softly.assertThatThrownBy(() -> set(jedis, "foo", "bar", "px", "1.5"))
                .hasMessage(NOT_AN_INTEGER);
        softly.assertThatThrownBy(() -> set(jedis, "foo", "bar", "exat", ""))
                .hasMessage(NOT_AN_INTEGER);
        softly.assertAll();
    }

    @TestTemplate
    public void nonPositiveExpirationIsRejected(Jedis jedis) {
        SoftAssertions softly = new SoftAssertions();
        for (String option : new String[]{"ex", "px", "exat", "pxat"}) {
            softly.assertThatThrownBy(() -> set(jedis, "foo", "bar", option, "0"))
                    .describedAs("SET foo bar %s 0", option)
                    .hasMessage(INVALID_EXPIRE);
            softly.assertThatThrownBy(() -> set(jedis, "foo", "bar", option, "-1"))
                    .describedAs("SET foo bar %s -1", option)
                    .hasMessage(INVALID_EXPIRE);
        }
        softly.assertAll();
    }

    @TestTemplate
    public void wrongNumberOfArgumentsIsNotASyntaxError(Jedis jedis) {
        String message = "ERR wrong number of arguments for 'set' command";
        assertThatThrownBy(() -> set(jedis)).hasMessage(message);
        assertThatThrownBy(() -> set(jedis, "foo")).hasMessage(message);
    }

    @TestTemplate
    public void getCombinesWithEveryOtherOption(Jedis jedis) {
        jedis.set("foo", "old");
        assertThat(setReply(jedis, "foo", "a", "get")).isEqualTo("old");
        assertThat(setReply(jedis, "foo", "b", "get", "get")).isEqualTo("a");
        //NX declines the write, yet GET still reports what was there
        assertThat(setReply(jedis, "foo", "c", "nx", "get")).isEqualTo("b");
        assertThat(setReply(jedis, "foo", "d", "xx", "get", "ex", "10")).isEqualTo("b");
        assertThat(jedis.get("foo")).isEqualTo("d");
        assertThat(jedis.ttl("foo")).isEqualTo(10L);
    }

    @TestTemplate
    public void aRejectedCommandWritesNothing(Jedis jedis) {
        jedis.set("foo", "original");
        assertThatThrownBy(() -> set(jedis, "foo", "replacement", "badoption"))
                .hasMessage(SYNTAX_ERROR);
        assertThatThrownBy(() -> set(jedis, "foo", "replacement", "ex", "0"))
                .hasMessage(INVALID_EXPIRE);
        assertThatThrownBy(() -> set(jedis, "foo", "replacement", "ex", "notanumber"))
                .hasMessage(NOT_AN_INTEGER);
        assertThat(jedis.get("foo")).isEqualTo("original");
        assertThat(jedis.ttl("foo")).isEqualTo(-1L);
    }
}
