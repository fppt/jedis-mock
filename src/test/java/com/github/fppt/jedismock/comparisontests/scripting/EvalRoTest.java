package com.github.fppt.jedismock.comparisontests.scripting;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import com.github.fppt.jedismock.operations.scripting.Script;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
class EvalRoTest {

    private static final String READ_ONLY_ERROR = "ERR Write commands are not allowed from read-only scripts.";

    @BeforeEach
    void setUp(Jedis jedis) {
        jedis.flushAll();
        jedis.scriptFlush();
    }

    @TestTemplate
    void successfulCase(Jedis jedis) {
        jedis.set("foo", "bar");
        assertThat(jedis.evalReadonly("return redis.call('get', KEYS[1])", List.of("foo"), List.of()))
                .isEqualTo("bar");
    }

    @TestTemplate
    void cannotRunWriteCommands(Jedis jedis) {
        jedis.set("foo", "bar");
        String script = "redis.call('del', KEYS[1])";
        assertThatThrownBy(() -> jedis.evalReadonly(script, List.of("foo"), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessage(READ_ONLY_ERROR + " script: " + Script.getScriptSHA(script) + ", on @user_script:1.");
        assertThat(jedis.get("foo")).isEqualTo("bar");
    }

    @TestTemplate
    void writeCommandReturnedDirectlyIsRejected(Jedis jedis) {
        String script = "return redis.call('set', 'x', 'y')";
        assertThatThrownBy(() -> jedis.evalReadonly(script, List.of("x"), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessage(READ_ONLY_ERROR + " script: " + Script.getScriptSHA(script) + ", on @user_script:1.");
        assertThat(jedis.get("x")).isNull();
    }

    @TestTemplate
    void writeCommandIsRejectedEvenWhenItWouldFailOnWrongType(Jedis jedis) {
        jedis.set("foo", "bar");
        String script = "return redis.call('lpush', 'foo', 'a')";
        assertThatThrownBy(() -> jedis.evalReadonly(script, List.of("foo"), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessage(READ_ONLY_ERROR + " script: " + Script.getScriptSHA(script) + ", on @user_script:1.");
    }

    @TestTemplate
    void pcallOfWriteCommandReturnsErrorTable(Jedis jedis) {
        String script = "local t = redis.pcall('set', 'x', 'y')\n"
                + "if t['err'] == '" + READ_ONLY_ERROR + "' then return 1 else return 0 end";
        assertThat(jedis.evalReadonly(script, List.of("x"), List.of())).isEqualTo(1L);
        assertThat(jedis.get("x")).isNull();
    }

    @TestTemplate
    void unknownCommandAndArityErrorsTakePrecedenceOverReadOnlyCheck(Jedis jedis) {
        assertThatThrownBy(() -> jedis.evalReadonly("return redis.call('set', 'x')", List.of("x"), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessageMatching("ERR Wrong number of args calling (Redis )?command from script script: .*");
        assertThatThrownBy(() -> jedis.evalReadonly("return redis.call('nosuchcommand', 'x')", List.of("x"), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessageMatching("ERR Unknown (Redis )?command called from script script: .*");
    }

    @TestTemplate
    void selectIsAllowed(Jedis jedis) {
        jedis.set("foo", "bar");
        assertThat(jedis.evalReadonly("redis.call('select', 1)\nreturn redis.call('get', 'foo')", List.of(), List.of()))
                .isNull();
        assertThat(jedis.get("foo")).isEqualTo("bar");
    }

    @TestTemplate
    void scriptIsCached(Jedis jedis) {
        String script = "return 1";
        assertThat(jedis.evalReadonly(script, List.of(), List.of())).isEqualTo(1L);
        String sha = Script.getScriptSHA(script);
        assertThat(jedis.scriptExists(sha)).isTrue();
        assertThat(jedis.evalsha(sha, 0)).isEqualTo(1L);
        assertThat(jedis.evalshaReadonly(sha, List.of(), List.of())).isEqualTo(1L);
    }

    @TestTemplate
    void numberOfKeysIsValidated(Jedis jedis) {
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.EVAL_RO, "return 1", "-1"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Number of keys can't be negative");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.EVAL_RO, "return 1", "2", "a"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Number of keys can't be greater than number of args");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.EVAL_RO, "return 1", "abc"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR value is not an integer or out of range");
    }

    @TestTemplate
    void arityIsValidated(Jedis jedis) {
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.EVAL_RO, "return 1"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR wrong number of arguments for 'eval_ro' command");
    }

    @TestTemplate
    void isNotAllowedFromScripts(Jedis jedis) {
        assertThatThrownBy(() -> jedis.eval("return redis.call('eval_ro', 'return 1', 0)", 0))
                .isInstanceOf(JedisDataException.class)
                .hasMessageMatching("ERR This (Redis )?command is not allowed from script script: .*");
        assertThatThrownBy(() -> jedis.eval("return redis.call('evalsha_ro', 'abc', 0)", 0))
                .isInstanceOf(JedisDataException.class)
                .hasMessageMatching("ERR This (Redis )?command is not allowed from script script: .*");
        assertThatThrownBy(() -> jedis.eval("return redis.call('evalsha', 'abc', 0)", 0))
                .isInstanceOf(JedisDataException.class)
                .hasMessageMatching("ERR This (Redis )?command is not allowed from script script: .*");
    }
}
