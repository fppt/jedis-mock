package com.github.fppt.jedismock.operations.scripting;

import com.github.fppt.jedismock.storage.ScriptingManager;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.DebugLib;

/**
 * A {@link DebugLib} whose per-instruction hook aborts the running script as
 * soon as a {@code SCRIPT KILL} has been requested.
 * <p>
 * LuaJ's interpreter invokes {@link #onInstruction} before every VM instruction
 * (see {@code LuaClosure.execute}), so this fires even inside a tight
 * {@code while true do end} loop — it is the one place Java code can run while
 * the script is otherwise stuck. Installing it via
 * {@code globals.load(new InterruptibleDebugLib(...))} replaces the default
 * debug library set up by {@code JsePlatform.standardGlobals()}; since that
 * default already incurs the per-instruction callback, checking a single
 * {@code volatile} flag adds no measurable overhead.
 */
public final class InterruptibleDebugLib extends DebugLib {
    private final ScriptingManager scriptingManager;

    public InterruptibleDebugLib(ScriptingManager scriptingManager) {
        this.scriptingManager = scriptingManager;
    }

    @Override
    public void onInstruction(int pc, Varargs v, int top) {
        if (scriptingManager.isKillRequested()) {
            throw new LuaError("Script killed by user with SCRIPT KILL...");
        }
        super.onInstruction(pc, v, top);
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
