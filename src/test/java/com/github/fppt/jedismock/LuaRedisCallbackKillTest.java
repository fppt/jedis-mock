package com.github.fppt.jedismock;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.scripting.LuaRedisCallback;
import com.github.fppt.jedismock.operations.scripting.ScriptKilledError;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.storage.ScriptingManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.luaj.vm2.LuaValue;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.Socket;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The kill signal must not be swallowed by {@code redis.pcall}. When a write is
 * dispatched after {@code SCRIPT KILL} was granted, the command bridge refuses
 * it by raising the kill error; a {@code pcall} that turned that error into an
 * ordinary {@code {err=...}} reply would let a script that returns the result
 * directly (no further Lua instruction for the kill hook to observe) finish
 * normally. The race is a few microseconds wide, so it is pinned down here
 * rather than through the socket. (Lives in this package for the
 * package-private {@link RedisClient} constructor.)
 */
class LuaRedisCallbackKillTest {

    private OperationExecutorState state;
    private LuaRedisCallback callback;

    @BeforeEach
    void setUp() throws IOException {
        Map<Integer, RedisBase> redisBases = new HashMap<>();
        redisBases.put(0, new RedisBase(Clock::systemDefaultZone));
        RedisClient client = new RedisClient(new RedisServer(), Mockito.mock(Socket.class), c -> { });
        state = new OperationExecutorState(client, redisBases);
        callback = new LuaRedisCallback(state);
        state.scriptingManager().start();
    }

    @AfterEach
    void tearDown() {
        state.scriptingManager().stop();
    }

    private static LuaValue command(String... words) {
        LuaValue[] values = new LuaValue[words.length];
        for (int i = 0; i < words.length; i++) {
            values[i] = LuaValue.valueOf(words[i]);
        }
        return LuaValue.listOf(values);
    }

    @Test
    void pcallReportsOrdinaryCommandErrorsAsTables() {
        LuaValue result = callback.pcall(command("incr"));
        assertThat(result.istable()).isTrue();
        assertThat(result.get("err").tojstring()).contains("Wrong number of args");
    }

    @Test
    void pcallRaisesTheKillInsteadOfReturningIt() {
        assertThat(state.scriptingManager().requestKill(ScriptingManager.ScriptType.SCRIPT))
                .isEqualTo(ScriptingManager.KillOutcome.KILLED);
        assertThatThrownBy(() -> callback.pcall(command("set", "k", "v")))
                .isInstanceOf(ScriptKilledError.class);
        assertThat(state.base().getValue(Slice.create("k"))).isNull();
    }

    @Test
    void callRaisesTheKillToo() {
        state.scriptingManager().requestKill(ScriptingManager.ScriptType.SCRIPT);
        assertThatThrownBy(() -> callback.call(command("set", "k", "v")))
                .isInstanceOf(ScriptKilledError.class)
                .extracting(e -> ((ScriptKilledError) e).getMessageObject().get("err").tojstring())
                .isEqualTo("Script killed by user with SCRIPT KILL...");
    }
}
