package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The exact error texts real Redis replies with when a sorted-set command is given
 * a score, weight or lexicographic bound it cannot parse.
 */
@ExtendWith(ComparisonBase.class)
public class TestSortedSetArgumentErrors {

    private static final String KEY = "zset";
    private static final String NOT_A_FLOAT = "ERR value is not a valid float";
    private static final String BOUND_NOT_A_FLOAT = "ERR min or max is not a float";
    private static final String WEIGHT_NOT_A_FLOAT = "ERR weight value is not a float";
    private static final String BAD_LEX_RANGE = "ERR min or max not valid string range item";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
        jedis.zadd(KEY, 1, "one");
        jedis.zadd(KEY, 2, "two");
    }

    @TestTemplate
    public void zaddRejectsUnparsableScore(Jedis jedis) {
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.ZADD, KEY, "v", "one"))
                .isInstanceOf(JedisDataException.class).hasMessage(NOT_A_FLOAT);
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.ZADD, KEY, "nan", "one"))
                .isInstanceOf(JedisDataException.class).hasMessage(NOT_A_FLOAT);
    }

    @TestTemplate
    public void zincrbyRejectsUnparsableIncrement(Jedis jedis) {
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.ZINCRBY, KEY, "v", "one"))
                .isInstanceOf(JedisDataException.class).hasMessage(NOT_A_FLOAT);
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.ZINCRBY, KEY, "nan", "one"))
                .isInstanceOf(JedisDataException.class).hasMessage(NOT_A_FLOAT);
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.ZINCRBY, "missing", "v", "one"))
                .isInstanceOf(JedisDataException.class).hasMessage(NOT_A_FLOAT);
    }

    @TestTemplate
    public void scoreRangeCommandsRejectUnparsableBounds(Jedis jedis) {
        assertThatThrownBy(() -> jedis.zrangeByScore(KEY, "v", "1"))
                .isInstanceOf(JedisDataException.class).hasMessage(BOUND_NOT_A_FLOAT);
        assertThatThrownBy(() -> jedis.zrangeByScore(KEY, "(v", "1"))
                .isInstanceOf(JedisDataException.class).hasMessage(BOUND_NOT_A_FLOAT);
        assertThatThrownBy(() -> jedis.zrangeByScore(KEY, "1", "v"))
                .isInstanceOf(JedisDataException.class).hasMessage(BOUND_NOT_A_FLOAT);
        assertThatThrownBy(() -> jedis.zrangeByScore(KEY, "1", "nan"))
                .isInstanceOf(JedisDataException.class).hasMessage(BOUND_NOT_A_FLOAT);
        assertThatThrownBy(() -> jedis.zrevrangeByScore(KEY, "v", "1"))
                .isInstanceOf(JedisDataException.class).hasMessage(BOUND_NOT_A_FLOAT);
        assertThatThrownBy(() -> jedis.zcount(KEY, "v", "1"))
                .isInstanceOf(JedisDataException.class).hasMessage(BOUND_NOT_A_FLOAT);
        assertThatThrownBy(() -> jedis.zremrangeByScore(KEY, "v", "1"))
                .isInstanceOf(JedisDataException.class).hasMessage(BOUND_NOT_A_FLOAT);
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.ZRANGE, KEY, "v", "1", "BYSCORE"))
                .isInstanceOf(JedisDataException.class).hasMessage(BOUND_NOT_A_FLOAT);
    }

    @TestTemplate
    public void storeCommandsRejectUnparsableWeights(Jedis jedis) {
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.ZUNIONSTORE, "dst", "1", KEY, "WEIGHTS", "v"))
                .isInstanceOf(JedisDataException.class).hasMessage(WEIGHT_NOT_A_FLOAT);
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.ZUNIONSTORE, "dst", "1", KEY, "WEIGHTS", "nan"))
                .isInstanceOf(JedisDataException.class).hasMessage(WEIGHT_NOT_A_FLOAT);
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.ZINTERSTORE, "dst", "1", KEY, "WEIGHTS", "v"))
                .isInstanceOf(JedisDataException.class).hasMessage(WEIGHT_NOT_A_FLOAT);
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.ZUNION, "1", KEY, "WEIGHTS", "nan"))
                .isInstanceOf(JedisDataException.class).hasMessage(WEIGHT_NOT_A_FLOAT);
    }

    @TestTemplate
    public void lexRangeCommandsRejectBoundsWithoutPrefix(Jedis jedis) {
        assertThatThrownBy(() -> jedis.zrangeByLex(KEY, "a", "b"))
                .isInstanceOf(JedisDataException.class).hasMessage(BAD_LEX_RANGE);
        assertThatThrownBy(() -> jedis.zrangeByLex(KEY, "[a", "b"))
                .isInstanceOf(JedisDataException.class).hasMessage(BAD_LEX_RANGE);
        assertThatThrownBy(() -> jedis.zrevrangeByLex(KEY, "b", "a"))
                .isInstanceOf(JedisDataException.class).hasMessage(BAD_LEX_RANGE);
        assertThatThrownBy(() -> jedis.zlexcount(KEY, "a", "b"))
                .isInstanceOf(JedisDataException.class).hasMessage(BAD_LEX_RANGE);
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.ZRANGE, KEY, "a", "b", "BYLEX"))
                .isInstanceOf(JedisDataException.class).hasMessage(BAD_LEX_RANGE);
    }
}
