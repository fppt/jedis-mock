package com.github.fppt.jedismock.comparisontests;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Argument-count validation for commands whose declared arity used to be
 * looser than real Redis.
 */
@ExtendWith(ComparisonBase.class)
public class ArityValidationTest {

    private static void assertArityError(Jedis jedis, Protocol.Command command, String... args) {
        assertThatThrownBy(() -> jedis.sendCommand(command, args))
                .isInstanceOf(JedisDataException.class)
                .hasMessage(String.format("ERR wrong number of arguments for '%s' command",
                        command.name().toLowerCase()));
    }

    @TestTemplate
    public void saddRequiresAtLeastOneMember(Jedis jedis) {
        assertArityError(jedis, Protocol.Command.SADD, "set");
    }

    @TestTemplate
    public void incrByRequiresExactlyTwoArguments(Jedis jedis) {
        assertArityError(jedis, Protocol.Command.INCRBY, "key");
        assertArityError(jedis, Protocol.Command.INCRBY, "key", "1", "extra");
    }

    @TestTemplate
    public void decrByRequiresExactlyTwoArguments(Jedis jedis) {
        assertArityError(jedis, Protocol.Command.DECRBY, "key");
        assertArityError(jedis, Protocol.Command.DECRBY, "key", "1", "extra");
    }

    @TestTemplate
    public void incrByFloatRequiresExactlyTwoArguments(Jedis jedis) {
        assertArityError(jedis, Protocol.Command.INCRBYFLOAT, "key");
        assertArityError(jedis, Protocol.Command.INCRBYFLOAT, "key", "1.5", "extra");
    }

    @TestTemplate
    public void setExRequiresExactlyThreeArguments(Jedis jedis) {
        assertArityError(jedis, Protocol.Command.SETEX, "key", "10");
        assertArityError(jedis, Protocol.Command.SETEX, "key", "10", "value", "extra");
    }

    @TestTemplate
    public void pSetExRequiresExactlyThreeArguments(Jedis jedis) {
        assertArityError(jedis, Protocol.Command.PSETEX, "key", "10000");
        assertArityError(jedis, Protocol.Command.PSETEX, "key", "10000", "value", "extra");
    }
}
