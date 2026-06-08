package com.github.fppt.jedismock.operations.scripting;

import com.github.fppt.jedismock.RedisServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Mock-only checks for the script-side behaviour exercised by the tcl
 * {@code unit/scripting} suite: a Redis status reply must reach Lua as a table
 * with an {@code ok} field, and WAIT/WAITAOF must be callable from a script
 * (scripts may never block) and return immediately on a replica-less mock.
 */
class ScriptingTypeConversionTest {
    private RedisServer server;
    private Jedis jedis;

    @BeforeEach
    void setUp() throws IOException {
        server = RedisServer.newRedisServer();
        server.start();
        jedis = new Jedis(server.getHost(), server.getBindPort());
    }

    @AfterEach
    void tearDown() throws IOException {
        jedis.close();
        server.stop();
    }

    @Test
    void statusReplyConvertsToLuaTableWithOkField() {
        //Mirrors "EVAL - Redis status reply -> Lua type conversion": SET returns
        //a status reply, which in Lua is a table {ok="OK"}, not a bare string.
        Object result = jedis.eval(
                "local foo = redis.pcall('set', KEYS[1], 'myval')\n"
                        + "return {type(foo), foo['ok']}",
                1, "mykey");

        assertThat(result).isEqualTo(Arrays.asList("table", "OK"));
    }

    @Test
    void scriptDoesNotBlockOnWait() {
        //Mirrors "EVAL - Scripts do not block on wait": no replicas -> 0.
        Object result = jedis.eval("return redis.pcall('wait', '1', '0')", 0);

        assertThat(result).isEqualTo(0L);
    }

    @Test
    void scriptDoesNotBlockOnWaitAof() {
        //Mirrors "EVAL - Scripts do not block on waitaof": no AOF, no replicas.
        Object result = jedis.eval("return redis.pcall('waitaof', '0', '1', '0')", 0);

        assertThat(result).isEqualTo(Arrays.asList(0L, 0L));
    }

    @Test
    void waitIsCallableDirectly() {
        assertThat(jedis.waitReplicas(0, 0L)).isZero();
    }

    @Test
    void waitAofReturnsZeroAcks() {
        //WAITAOF numlocal numreplicas timeout -> [localAcks, replicaAcks].
        Object result = jedis.sendCommand(() -> "WAITAOF".getBytes(),
                "0".getBytes(), "0".getBytes(), "0".getBytes());

        assertThat(result).isEqualTo(Arrays.asList(0L, 0L));
    }

    @Test
    void redisCallWithNoArgumentsIsAnError() {
        //"EVAL - No arguments to redis.call/pcall is considered an error".
        assertThatThrownBy(() -> jedis.eval("return redis.call()", 0))
                .hasMessageContaining("one argument");
    }

    @Test
    void redisCallOnUnknownCommandIsAnError() {
        //"EVAL - redis.call variant raises a Lua error on Redis cmd error":
        //nosuchcommand -> "Unknown Redis command called from script".
        assertThatThrownBy(() -> jedis.eval("redis.call('nosuchcommand')", 0))
                .hasMessageContaining("Unknown Redis");
    }

    @Test
    void redisCallWithWrongArityIsAnError() {
        //GET takes exactly one key; extra args are an arity error, surfaced to
        //the script as "Wrong number of args calling Redis command from script".
        assertThatThrownBy(() -> jedis.eval("redis.call('get','a','b','c')", 0))
                .hasMessageContaining("number of args");
    }

    @Test
    void redisCallAgainstWrongTypeReportsCanonicalWrongType() {
        //"EVAL - ... raises a Lua error on Redis cmd error": LPUSH against a
        //string key yields the canonical WRONGTYPE message.
        jedis.set("foo", "bar");
        assertThatThrownBy(() -> jedis.eval("redis.call('lpush',KEYS[1],'val')", 1, "foo"))
                .hasMessageContaining("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    @Test
    void negativeKeyCountIsAnErrorNotACrash() {
        //"Verify negative arg count is error instead of crash (issue #1842)":
        //a negative numkeys must be rejected cleanly, not leak a Java exception.
        //Sent raw so the client doesn't reject the negative count first.
        assertThatThrownBy(() -> jedis.sendCommand(() -> "EVAL".getBytes(),
                "return 'hello'", "-12"))
                .hasMessage("ERR Number of keys can't be negative");
    }

    @Test
    void redisCallWithWrongArityIsPrefixedWithErr() {
        //"Scripts can handle commands with incorrect arity": the reply is a
        //single Redis error, ERR-prefixed, without any Java/luaj wrapper.
        assertThatThrownBy(() -> jedis.eval("redis.call('set','invalid')", 0))
                .hasMessage("ERR Wrong number of args calling Redis command from script");
    }

    @Test
    void sha1hexWithoutArgumentReportsArityError() {
        //"Functions in the Redis namespace are able to report errors":
        //redis.sha1hex() with no argument must not crash with a NullPointer.
        assertThatThrownBy(() -> jedis.eval("redis.sha1hex()", 0))
                .hasMessageContaining("wrong number");
    }

    @Test
    void clusterCommandIsNotAllowedFromScript() {
        //"CLUSTER RESET can not be invoke from within a script": CLUSTER is
        //flagged no-script, distinct from a genuinely unknown command.
        assertThatThrownBy(() -> jedis.eval("redis.call('cluster', 'reset', 'hard')", 0))
                .hasMessageContaining("command is not allowed");
    }

    @Test
    void callingANilValueUsesReferenceLuaWording() {
        //"Binary code loading failed": loadstring of a binary dump yields nil in
        //luaj; calling it must report the reference-Lua "a nil value" wording.
        assertThatThrownBy(() -> jedis.eval(
                "return loadstring(string.dump(function() return 1 end))()", 0))
                .hasMessageContaining("attempt to call a nil value");
    }

    @Test
    void scriptDoesNotBlockOnXreadWithBlockOption() {
        //"EVAL - Scripts do not block on XREAD with BLOCK option": a blocking
        //XREAD inside a script must return immediately, never block.
        jedis.xadd("st", redis.clients.jedis.StreamEntryID.NEW_ENTRY,
                Collections.singletonMap("a", "1"));

        //BLOCK 0 from '$' (only new entries): nothing pending -> empty (and,
        //crucially, returns instead of blocking forever).
        Object noNewEntries = jedis.eval("return redis.pcall('xread','BLOCK',0,'STREAMS','st','$')", 0);
        assertThat(noNewEntries == null || ((java.util.List<?>) noNewEntries).isEmpty()).isTrue();
        //BLOCK 0 from 0-0: the existing entry is returned, still without blocking.
        assertThat(jedis.eval("return redis.pcall('xread','BLOCK',0,'STREAMS','st','0-0')", 0))
                .isNotNull();
    }
}
