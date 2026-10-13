package com.github.fppt.jedismock.operations.scripting;

import com.github.fppt.jedismock.storage.ScriptingManager;
import org.luaj.vm2.LuaClosure;
import org.luaj.vm2.LuaFunction;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Prototype;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.DebugLib;

import java.util.Arrays;

/**
 * A {@link DebugLib} whose per-instruction hook aborts the running script as
 * soon as a {@code SCRIPT/FUNCTION KILL} has been requested.
 * <p>
 * LuaJ's interpreter invokes {@link #onInstruction} before every VM instruction
 * (see {@code LuaClosure.execute}), so this fires even inside a tight
 * {@code while true do end} loop — it is the one place Java code can run while
 * the script is otherwise stuck. Installing it via
 * {@code globals.load(new InterruptibleDebugLib(...))} replaces the default
 * debug library set up by {@code JsePlatform.standardGlobals()}; since that
 * default already incurs the per-instruction callback, checking a single
 * {@code volatile} flag adds no measurable overhead.
 * <p>
 * The same hooks also track the most recently executed instruction of a
 * <em>user</em> chunk ({@code @user_script} / {@code @user_function}), see
 * {@link #lastUserLocation()}. Real Redis reports the user line at which a
 * failing {@code redis.call} was made; luaj, however, compiles
 * {@code return redis.call(...)} to a tail call that pops the user frame
 * before the command runs, so the error it raises carries no location. The
 * last user instruction executed is exactly that call.
 */
public class InterruptibleDebugLib extends DebugLib implements ScriptingManager.UserLocationTracker {
    private static final String USER_CHUNK_PREFIX = "@user_";

    private final ScriptingManager scriptingManager;

    //A mirror of luaj's own call stack (one entry per onCall, popped by
    //onReturn), recording for each frame whether it runs a user chunk.
    private boolean[] userFrames = new boolean[16];
    private LuaClosure[] frames = new LuaClosure[16];
    private int depth = 0;

    //The user instruction executed most recently.
    private LuaClosure lastUserClosure = null;
    private int lastUserPc = -1;

    public InterruptibleDebugLib(ScriptingManager scriptingManager) {
        this.scriptingManager = scriptingManager;
    }

    @Override
    public void onCall(LuaFunction f) {
        push(null, false);
        super.onCall(f);
    }

    @Override
    public void onCall(LuaClosure c, Varargs varargs, LuaValue[] stack) {
        boolean user = c.p.source != null && c.p.source.tojstring().startsWith(USER_CHUNK_PREFIX);
        push(c, user);
        if (user) {
            scriptingManager.setUserLocationTracker(this);
        }
        super.onCall(c, varargs, stack);
    }

    @Override
    public void onInstruction(int pc, Varargs v, int top) {
        if (scriptingManager.isKillRequested()) {
            throw killedError();
        }
        if (depth > 0 && userFrames[depth - 1]) {
            lastUserClosure = frames[depth - 1];
            lastUserPc = pc;
        }
        super.onInstruction(pc, v, top);
    }

    @Override
    public void onReturn() {
        if (depth > 0) {
            frames[--depth] = null;
        }
        super.onReturn();
    }

    private void push(LuaClosure closure, boolean user) {
        if (depth == frames.length) {
            frames = Arrays.copyOf(frames, depth * 2);
            userFrames = Arrays.copyOf(userFrames, depth * 2);
        }
        frames[depth] = closure;
        userFrames[depth] = user;
        depth++;
    }

    @Override
    public String lastUserLocation() {
        LuaClosure closure = lastUserClosure;
        if (closure == null) {
            return null;
        }
        Prototype p = closure.p;
        int pc = lastUserPc;
        String line = p.lineinfo != null && pc >= 0 && pc < p.lineinfo.length ? String.valueOf(p.lineinfo[pc]) : "?";
        return p.source.tojstring() + ":" + line;
    }

    /**
     * The error a killed script dies with. Also raised by the redis.call
     * bridge when a write is dispatched after the kill was granted, so the
     * script aborts before the write instead of after it.
     */
    public static ScriptKilledError killedError() {
        return new ScriptKilledError();
    }

    /**
     * {@link org.luaj.vm2.LuaValue} defines {@code equals} as identity
     * ({@code this == obj}) but no {@code hashCode}. Make both explicit and
     * mutually consistent (identity semantics) so SpotBugs is satisfied; each
     * debug-lib instance is a distinct object anyway.
     */
    @Override
    public boolean equals(Object obj) {
        return this == obj;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }
}
