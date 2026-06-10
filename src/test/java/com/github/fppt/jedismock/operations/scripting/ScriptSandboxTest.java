package com.github.fppt.jedismock.operations.scripting;

import com.github.fppt.jedismock.RedisServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Mock-only checks for the Lua sandbox exercised by the tcl {@code unit/scripting}
 * suite: a read-only global environment with access protection, a read-only
 * {@code redis} table, guarded (set|get)metatable, removed dangerous globals and
 * an {@code os} table reduced to {@code os.clock}.
 */
class ScriptSandboxTest {
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
    void readingUndeclaredGlobalIsRejected() {
        assertThatThrownBy(() -> jedis.eval("return a", 0))
                .hasMessageContaining("attempted to access nonexistent global variable 'a'");
    }

    @Test
    void writingUndeclaredGlobalIsReadonly() {
        assertThatThrownBy(() -> jedis.eval("a=10", 0))
                .hasMessageContaining("Attempt to modify a readonly table");
    }

    @Test
    void overwritingExistingGlobalIsReadonly() {
        assertThatThrownBy(() -> jedis.eval("redis = function() return 1 end", 0))
                .hasMessageContaining("Attempt to modify a readonly table");
        assertThatThrownBy(() -> jedis.eval("_G = {}", 0))
                .hasMessageContaining("Attempt to modify a readonly table");
    }

    @Test
    void setmetatableOnGlobalsIsReadonly() {
        assertThatThrownBy(() -> jedis.eval("setmetatable(_G, {})", 0))
                .hasMessageContaining("Attempt to modify a readonly table");
    }

    @Test
    void getmetatableOfGlobalsIsReadonly() {
        assertThatThrownBy(() -> jedis.eval("local g = getmetatable(_G)\ng.__index = {}", 0))
                .hasMessageContaining("Attempt to modify a readonly table");
    }

    @Test
    void redisTableIsReadonly() {
        assertThatThrownBy(() -> jedis.eval("redis.call = function() return 1 end", 0))
                .hasMessageContaining("Attempt to modify a readonly table");
    }

    @Test
    void basicTypeMetatablesAreNotMutable() {
        String[] scripts = {
                "getmetatable(nil).__index = function() return 1 end",
                "getmetatable('').__index = function() return 1 end",
                "getmetatable(123.222).__index = function() return 1 end",
                "getmetatable(true).__index = function() return 1 end",
                "getmetatable(function() return 1 end).__index = function() return 1 end",
        };
        for (String script : scripts) {
            assertThatThrownBy(() -> jedis.eval(script, 0))
                    .as("script: %s", script)
                    .satisfiesAnyOf(
                            e -> assertThat(e).hasMessageContaining("attempt to index a nil value"),
                            e -> assertThat(e).hasMessageContaining("Attempt to modify a readonly table"));
        }
    }

    @Test
    void dangerousGlobalsAreRemoved() {
        for (String name : new String[]{"loadfile", "dofile", "print", "setfenv", "getfenv", "newproxy"}) {
            assertThatThrownBy(() -> jedis.eval(name + "('x')", 0))
                    .as("global: %s", name)
                    .hasMessageContaining("nonexistent global variable '" + name + "'");
        }
    }

    @Test
    void osIsReducedToClock() {
        //Only os.clock survives; the dangerous methods are gone (os.exit() then
        //fails as a call on nil -- luaj does not name the field, so we just verify
        //the field is absent and the call raises).
        assertThat(jedis.eval("local n=0\nfor k,v in pairs(os) do n=n+1 end\nreturn n", 0)).isEqualTo(1L);
        assertThat(jedis.eval("return type(os.clock)", 0)).isEqualTo("function");
        assertThat(jedis.eval("return type(os.exit)", 0)).isEqualTo("nil");
        assertThatThrownBy(() -> jedis.eval("os.exit()", 0))
                .hasMessageContaining("attempt to call a nil value");
    }

    @Test
    void osClockReturnsADouble() {
        //{double=...} convention is used by the tcl os.clock timing test.
        Object r = jedis.eval("return {double = os.clock()}", 0);
        assertThat(Double.parseDouble((String) r)).isGreaterThanOrEqualTo(0.0);
    }

    @Test
    void setmetatableOnLocalTableStillWorks() {
        //"EVAL - Return table with a metatable that raise error": setmetatable on a
        //script-local table must keep working.
        Object r = jedis.eval(
                "local a = {}\nsetmetatable(a, {__index=function() foo() end})\nreturn a", 0);
        assertThat(r == null || ((java.util.List<?>) r).isEmpty()).isTrue();
    }

    @Test
    void existingGlobalsRemainReadable() {
        assertThat(jedis.eval("return type(pcall)", 0)).isEqualTo("function");
        assertThat(jedis.eval("return #KEYS", 1, "k")).isEqualTo(1L);
    }

    @Test
    void sandboxedRunawayScriptIsStillInterruptible() {
        //Regression guard: the script runs in a sandbox environment, but it must
        //remain killable. luaj only wires the per-instruction interrupt hook when
        //the chunk's environment is a Globals instance, so the sandbox env is
        //applied via the _ENV upvalue rather than as the load environment -- if
        //that regresses, SCRIPT KILL can never abort a runaway loop and the whole
        //server wedges (caught here by the preemptive timeout).
        jedis.configSet("lua-time-limit", "100");
        Jedis busyClient = new Jedis(server.getHost(), server.getBindPort(), 1_000_000);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            pool.submit(() -> {
                try {
                    busyClient.eval("while true do end", 0);
                } catch (Exception ignored) {
                    //aborted by SCRIPT KILL
                }
            });
            assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
                //SCRIPT KILL replies NOTBUSY until the runaway script has actually
                //started, so retry on NOTBUSY only (any other error is a real
                //failure) rather than guessing a fixed startup delay. The mock's
                //SCRIPT KILL blocks until the script has stopped, so once it
                //succeeds PING is immediately PONG. The outer preemptive timeout
                //is the deadlock guard: if SCRIPT KILL ever wedged, it fires.
                while (true) {
                    try {
                        jedis.scriptKill();
                        break;
                    } catch (JedisDataException e) {
                        if (e.getMessage() == null || !e.getMessage().contains("NOTBUSY")) {
                            throw e;
                        }
                        Thread.sleep(50);
                    }
                }
                assertThat(jedis.ping()).isEqualTo("PONG");
            });
        } finally {
            pool.shutdownNow();
            busyClient.close();
        }
    }

    @Test
    void pcallCatchesUndeclaredGlobalAccess() {
        //The re-enabled "LUA test pcall with error": foo access raises inside pcall.
        Object r = jedis.eval(
                "local status, res = pcall(function() return foo end)\n"
                        + "return 'status: ' .. tostring(status) .. ' result: ' .. res", 0);
        assertThat((String) r)
                .startsWith("status: false result:")
                .endsWith("nonexistent global variable 'foo'")
                //The error caught by pcall must not carry a luaj stack traceback.
                .doesNotContain("stack traceback");
    }
}
