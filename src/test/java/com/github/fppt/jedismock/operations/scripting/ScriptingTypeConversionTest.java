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
        //nosuchcommand -> "Unknown command called from script" (Valkey wording).
        assertThatThrownBy(() -> jedis.eval("redis.call('nosuchcommand')", 0))
                .hasMessageContaining("Unknown command");
    }

    @Test
    void redisCallWithWrongArityIsAnError() {
        //GET takes exactly one key; extra args are an arity error, surfaced to
        //the script as "Wrong number of args calling command from script".
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
                .hasMessageStartingWith("ERR Wrong number of args calling command from script");
        //INCR with no key signals the arity error by throwing rather than
        //returning an error reply; it must be normalised the same way.
        assertThatThrownBy(() -> jedis.eval("redis.call('incr')", 0))
                .hasMessageStartingWith("ERR Wrong number of args calling command from script");
    }

    @Test
    void statusReplyIsASimpleStringReply() {
        //"LUA redis.status_reply API": redis.status_reply(x) is a status (simple
        //string) reply, "+x", not a bulk string. Read raw to see the type byte.
        Object raw = jedis.sendCommand(() -> "EVAL".getBytes(),
                "return redis.status_reply('MY_OK_CODE custom msg')", "0");
        //Jedis decodes a simple-string reply to the bare payload.
        assertThat(new String((byte[]) raw)).isEqualTo("MY_OK_CODE custom msg");
    }

    @Test
    void errorReplyTablePassedToErrorIsUnwrappedAndSanitized() {
        //"LUA redis.error_reply API with CRLF injection attempt": error() of an
        //{err=...} table uses the err field, with CR/LF collapsed to spaces so it
        //can't inject a second RESP line.
        assertThatThrownBy(() -> jedis.eval("error(redis.error_reply('X\\r\\n+INJECTED'))", 0))
                .hasMessageStartingWith("ERR X  +INJECTED");
    }

    @Test
    void explicitErrorCallFormatsLikeRedis() {
        //"EVAL - explicit error() call handling".
        assertThatThrownBy(() -> jedis.eval("error('simple string error')", 0))
                .hasMessageStartingWith("ERR user_script:1: simple string error script: ");
        assertThatThrownBy(() -> jedis.eval("error({err='ERR table error'})", 0))
                .hasMessageStartingWith("ERR table error script: ");
        assertThatThrownBy(() -> jedis.eval("error({})", 0))
                .hasMessageStartingWith("ERR unknown error script: ");
    }

    @Test
    void scriptContextReportsTheActualErrorLine() {
        //The "script: <sha>, on @user_script:N." context must carry the real line,
        //not a hard-coded 1. The error() is on line 3 of the script.
        assertThatThrownBy(() -> jedis.eval("local a = 1\nlocal b = 2\nerror('boom')", 0))
                .hasMessageContaining("user_script:3: boom")
                .hasMessageEndingWith("on @user_script:3.");
        //A redis.call failure on line 2 keeps its line through the vm-error unwrap.
        assertThatThrownBy(() -> jedis.eval("local a = 1\nredis.call('incr')", 0))
                .hasMessageEndingWith("on @user_script:2.");
    }

    @Test
    void nonStringOrIntegerCallArgumentIsRejected() {
        //"LUA test pcall with non string/integer arg": a table argument to
        //redis.call is rejected before dispatch...
        assertThatThrownBy(() -> jedis.eval("local x={}\nreturn redis.call('ping', x)", 0))
                .hasMessageStartingWith("ERR Command arguments must be strings or integers");
        //...and the next command still works (cached argv survived).
        assertThat(jedis.eval("return redis.call('ping','asdf')", 0)).isEqualTo("asdf");
    }

    @Test
    void pcallOfAPlainFunctionSucceeds() {
        //"LUA test pcall": pcall of a non-failing function yields (true, result).
        Object result = jedis.eval(
                "local status, res = pcall(function() return 1 end)\n"
                        + "return 'status: ' .. tostring(status) .. ' result: ' .. res", 0);
        assertThat(result).isEqualTo("status: true result: 1");
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
