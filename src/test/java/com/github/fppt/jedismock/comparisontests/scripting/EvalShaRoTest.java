package com.github.fppt.jedismock.comparisontests.scripting;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import com.github.fppt.jedismock.operations.scripting.Script;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisNoScriptException;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
class EvalShaRoTest {

    @BeforeEach
    void setUp(Jedis jedis) {
        jedis.flushAll();
        jedis.scriptFlush();
    }

    @TestTemplate
    void worksWithLowercaseAndUppercaseSha(Jedis jedis) {
        jedis.set("mykey", "myval");
        String script = "return redis.call('get', KEYS[1])";
        assertThat(jedis.eval(script, List.of("mykey"), List.of())).isEqualTo("myval");
        String sha = Script.getScriptSHA(script);
        assertThat(jedis.evalshaReadonly(sha.toLowerCase(), List.of("mykey"), List.of())).isEqualTo("myval");
        assertThat(jedis.evalshaReadonly(sha.toUpperCase(), List.of("mykey"), List.of())).isEqualTo("myval");
    }

    @TestTemplate
    void worksAfterScriptLoad(Jedis jedis) {
        String sha = jedis.scriptLoad("return 'Hello, ' .. ARGV[1] .. '!'");
        assertThat(jedis.evalshaReadonly(sha, List.of(), List.of("world"))).isEqualTo("Hello, world!");
    }

    @TestTemplate
    void unknownShaGivesNoScriptError(Jedis jedis) {
        assertThatThrownBy(() -> jedis.evalshaReadonly("abc", List.of(), List.of()))
                .isInstanceOf(JedisNoScriptException.class)
                .hasMessage("NOSCRIPT No matching script. Please use EVAL.");
    }

    @TestTemplate
    void cannotRunWriteCommands(Jedis jedis) {
        String sha = jedis.scriptLoad("return redis.call('set', KEYS[1], 'v')");
        assertThatThrownBy(() -> jedis.evalshaReadonly(sha, List.of("x"), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Write commands are not allowed from read-only scripts. script: " + sha + ", on @user_script:1.");
        assertThat(jedis.get("x")).isNull();
        //The same cached script may still write through plain EVALSHA.
        assertThat(jedis.evalsha(sha, List.of("x"), List.of())).isEqualTo("OK");
        assertThat(jedis.get("x")).isEqualTo("v");
    }

    @TestTemplate
    void arityIsValidated(Jedis jedis) {
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.EVALSHA_RO, "abc"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR wrong number of arguments for 'evalsha_ro' command");
    }
}
