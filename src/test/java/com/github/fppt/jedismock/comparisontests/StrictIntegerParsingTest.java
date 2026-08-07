package com.github.fppt.jedismock.comparisontests;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Redis parses integer arguments with {@code string2ll}, which is stricter than
 * Java's {@link Long#parseLong}: a leading plus, leading zeroes, {@code -0} and
 * surrounding whitespace are all rejected. These are shared by every command
 * that takes an integer, so they are tested once here rather than per command.
 */
@ExtendWith(ComparisonBase.class)
public class StrictIntegerParsingTest {

    private static final String NOT_AN_INTEGER = "ERR value is not an integer or out of range";
    private static final String NOT_AN_OFFSET = "ERR bit offset is not an integer or out of range";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
        jedis.set("n", "10");
        jedis.rpush("mylist", "a", "b", "c");
    }

    @TestTemplate
    public void leadingPlusIsRejected(Jedis jedis) {
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.INCRBY, "n", "+1"))
                .hasMessage(NOT_AN_INTEGER);
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.DECRBY, "n", "+1"))
                .hasMessage(NOT_AN_INTEGER);
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.EXPIRE, "n", "+100"))
                .hasMessage(NOT_AN_INTEGER);
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.LRANGE, "mylist", "+0", "-1"))
                .hasMessage(NOT_AN_INTEGER);
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SETBIT, "bk", "+1", "1"))
                .hasMessage(NOT_AN_OFFSET);
    }

    @TestTemplate
    public void leadingZeroIsRejected(Jedis jedis) {
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.INCRBY, "n", "01"))
                .hasMessage(NOT_AN_INTEGER);
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.EXPIRE, "n", "01"))
                .hasMessage(NOT_AN_INTEGER);
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.LRANGE, "mylist", "00", "-1"))
                .hasMessage(NOT_AN_INTEGER);
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.GETBIT, "bk", "01"))
                .hasMessage(NOT_AN_OFFSET);
    }

    @TestTemplate
    public void negativeZeroAndWhitespaceAreRejected(Jedis jedis) {
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.INCRBY, "n", "-0"))
                .hasMessage(NOT_AN_INTEGER);
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.INCRBY, "n", " 1"))
                .hasMessage(NOT_AN_INTEGER);
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.INCRBY, "n", "1 "))
                .hasMessage(NOT_AN_INTEGER);
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.INCRBY, "n", ""))
                .hasMessage(NOT_AN_INTEGER);
    }

    @TestTemplate
    public void wellFormedIntegersAreStillAccepted(Jedis jedis) {
        assertThat(jedis.incrBy("n", -5)).isEqualTo(5);
        assertThat(jedis.incrBy("n", 0)).isEqualTo(5);
        assertThat(jedis.getrange("n", 0, -1)).isEqualTo("5");
        assertThat(jedis.lrange("mylist", 0, -1)).containsExactly("a", "b", "c");
        assertThat(jedis.setbit("bk", 0, true)).isFalse();
    }
}
