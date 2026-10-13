package com.github.fppt.jedismock.operations.scripting;

import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaValue;

/**
 * The error a script dies with after {@code SCRIPT KILL} / {@code FUNCTION KILL}
 * (or {@code SHUTDOWN NOSAVE}) was granted. It is raised by the per-instruction
 * hook of {@link InterruptibleDebugLib}, and by the command bridge when a write
 * is dispatched after the kill. It has its own type so that {@code redis.pcall}
 * can let it through instead of turning it into an ordinary {@code {err=...}}
 * reply: the kill must not be catchable from the script.
 */
public final class ScriptKilledError extends LuaError {

    public static final String MESSAGE = "Script killed by user with SCRIPT KILL...";

    private static final long serialVersionUID = 1L;

    public ScriptKilledError() {
        super(LuaValue.tableOf(new LuaValue[]{LuaValue.valueOf("err"), LuaValue.valueOf(MESSAGE)}));
    }
}
