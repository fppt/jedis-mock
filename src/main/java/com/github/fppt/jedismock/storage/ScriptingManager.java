package com.github.fppt.jedismock.storage;

import com.github.fppt.jedismock.datastructures.Slice;

import java.util.ArrayList;
import java.util.List;

/**
 * Server-wide coordination for Lua script execution. It serves two purposes:
 * letting a {@code SCRIPT KILL} on one connection abort a script running on
 * another, and letting other connections reply {@code -BUSY} once a script has
 * run longer than {@code lua-time-limit}.
 * <p>
 * Scripts are serialized by the global data lock
 * ({@link OperationExecutorState#lock()}), so at most one runs at a time;
 * {@link #start()} and {@link #stop()} are therefore called by that single
 * script thread while it holds the lock. Everything else must work <em>while</em>
 * the script still holds that lock, so the remaining methods never touch it:
 * the readers ({@link #isBusy()}, {@link #isRunning()}, {@link #isKillRequested()},
 * {@link #awaitStopped()}, ...) are lock-free and rely on {@code volatile}
 * visibility, while the state transitions ({@code start}/{@code stop},
 * {@link #markWrite()}, {@link #requestKill(ScriptType)}) are serialized on this
 * object's own monitor. That monitor is only ever held for a few field
 * assignments, never while waiting, so it cannot deadlock against the data lock.
 * <p>
 * The transitions must be atomic because a kill races the script it targets:
 * whether a write is dispatched before or after the kill decides between
 * "the script is UNKILLABLE" and "the script aborts without writing", and no
 * interleaving may both abort the script and let the write through.
 */
public final class ScriptingManager {
    /**
     * Default {@code lua-time-limit}, matching real Redis (5 seconds). A value of
     * {@code 0} disables the limit (a script never becomes BUSY).
     */
    private static final long DEFAULT_LUA_TIME_LIMIT_MILLIS = 5000L;

    private volatile boolean running = false;
    private volatile ScriptType scriptType = null;
    private volatile RunningFunctionInfo functionInfo = null;
    private volatile long startNanos = 0L;
    private volatile boolean killRequested = false;
    private volatile boolean writeDirty = false;
    private volatile long luaTimeLimitMillis = DEFAULT_LUA_TIME_LIMIT_MILLIS;

    /**
     * Mark the start of a script run, recording the start time and clearing any
     * stale kill request left over from a previous script/function.
     */
    public void start() {
        start(System.nanoTime(), ScriptType.SCRIPT, null);
    }

    /**
     * Mark the start of a function run, recording the start time and clearing any
     * stale kill request left over from a previous script/function.
     *
     * @param invokingFunction the name of the library function being invoked
     * @param invokingOperation the name of the Redis operation that invoked the function (e.g. "FCALL")
     * @param invokingParams the parameters passed to the Redis operation
     */
    public void startFunction(String invokingFunction, String invokingOperation, List<Slice> invokingParams) {
        long startNanos = System.nanoTime();
        RunningFunctionInfo functionInfo = new RunningFunctionInfo(invokingFunction, invokingOperation, invokingParams, startNanos);
        start(startNanos, ScriptType.FUNCTION, functionInfo);
    }

    private synchronized void start(long startNanos, ScriptType scriptType, RunningFunctionInfo functionInfo) {
        killRequested = false;
        writeDirty = false;
        this.startNanos = startNanos;
        running = true;
        this.scriptType = scriptType;
        this.functionInfo = functionInfo;
    }

    /**
     * Mark the end of a script run (normal completion, error, or kill).
     */
    public synchronized void stop() {
        killRequested = false;
        writeDirty = false;
        running = false;
        scriptType = null;
        functionInfo = null;
    }

    /**
     * Request termination of the currently running script, applying the same
     * checks in the same order as {@code scriptKill} in real Redis: nothing
     * running, then a script that already wrote (unkillable, whichever KILL
     * variant was used), then a {@code SCRIPT KILL} / {@code FUNCTION KILL}
     * mismatch. The check and the kill are one atomic step, so a write that
     * is dispatched concurrently either makes the script unkillable or is
     * refused by {@link #markWrite()}, never both aborted and executed.
     *
     * @param expectedScriptType the type the KILL variant is allowed to kill
     */
    public synchronized KillOutcome requestKill(ScriptType expectedScriptType) {
        if (!running) {
            return KillOutcome.NOT_BUSY;
        }
        if (writeDirty) {
            return KillOutcome.UNKILLABLE;
        }
        if (scriptType != expectedScriptType) {
            return KillOutcome.WRONG_TYPE;
        }
        killRequested = true;
        return KillOutcome.KILLED;
    }

    /**
     * Record that the running script is about to dispatch a write command.
     * Called by the script thread once the command passed its arity check and
     * is about to run, which is also when real Redis sets its write-dirty flag:
     * a write rejected for arity leaves the script killable, a write that fails
     * while executing does not.
     *
     * @return {@code true} if the write may proceed (the script is now
     * unkillable); {@code false} if a kill has already been granted, in which
     * case the script must abort instead of executing the write.
     */
    public synchronized boolean markWrite() {
        if (killRequested) {
            return false;
        }
        writeDirty = true;
        return true;
    }

    /**
     * @return whether the running script has dispatched a write command, in
     * which case {@code SCRIPT/FUNCTION KILL} refuses with {@code -UNKILLABLE}
     * and the server stays busy until the script ends by itself. Always
     * {@code false} when nothing is running.
     */
    public boolean hasWritten() {
        return writeDirty;
    }

    /**
     * @return whether a {@code SCRIPT/FUNCTION KILL} has been requested for the running
     * script/function. Polled from the LuaJ instruction hook
     * ({@code InterruptibleDebugLib}) so the script/function aborts promptly.
     */
    public boolean isKillRequested() {
        return killRequested;
    }

    /**
     * Block until the running script has actually stopped (its thread has called
     * {@link #stop()}). Used by {@code SCRIPT KILL} so it replies {@code OK} only
     * once the script is truly gone — matching real Redis, where the killer never
     * races the aborting script. The script aborts at its next LuaJ instruction
     * hook, so this returns almost immediately; it holds no lock, so it cannot
     * deadlock against the script thread that still owns the data lock.
     */
    public void awaitStopped() {
        while (running) {
            try {
                Thread.sleep(1L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * @return {@code true} if a script is currently running and has already run
     * longer than {@code lua-time-limit}, so other clients should be answered
     * with {@code -BUSY}. Always {@code false} when the limit is {@code 0}.
     */
    public boolean isBusy() {
        if (!running) {
            return false;
        }
        long limit = luaTimeLimitMillis;
        return limit > 0 && (System.nanoTime() - startNanos) >= limit * 1_000_000L;
    }

    /**
     * @return whether a script is currently running (regardless of how long).
     */
    public boolean isRunning() {
        return running;
    }

    public long getLuaTimeLimitMillis() {
        return luaTimeLimitMillis;
    }

    public void setLuaTimeLimitMillis(long luaTimeLimitMillis) {
        this.luaTimeLimitMillis = luaTimeLimitMillis;
    }

    public ScriptType getRunningScriptType() {
        return scriptType;
    }

    public RunningFunctionInfo getRunningFunctionInfo() {
        return functionInfo;
    }

    public static class RunningFunctionInfo {
        private final String invokingFunction;
        private final String invokingOperation;
        private final List<Slice> invokingParams;
        private final long startNanos;

        public RunningFunctionInfo(String invokingFunction, String invokingOperation, List<Slice> invokingParams, long startNanos) {
            this.invokingFunction = invokingFunction;
            this.invokingOperation = invokingOperation;
            this.invokingParams = invokingParams;
            this.startNanos = startNanos;
        }

        public String getInvokingFunction() {
            return invokingFunction;
        }

        public List<Slice> getInvokingCommand() {
            List<Slice> command = new ArrayList<>(invokingParams.size() + 1);
            command.add(Slice.create(invokingOperation));
            command.addAll(invokingParams);
            return command;
        }

        public long getStartNanos() {
            return startNanos;
        }
    }

    public enum ScriptType {
        SCRIPT,
        FUNCTION,
    }

    /**
     * Result of {@link #requestKill(ScriptType)}, in the order the checks are made.
     */
    public enum KillOutcome {
        /** Nothing is running: reply {@code -NOTBUSY}. */
        NOT_BUSY,
        /** The script already wrote: reply {@code -UNKILLABLE}, it runs to completion. */
        UNKILLABLE,
        /** A function is running but SCRIPT KILL was used, or vice versa: the caller falls through to the BUSY reply. */
        WRONG_TYPE,
        /** The kill has been granted: the script aborts at its next instruction. */
        KILLED,
    }
}
