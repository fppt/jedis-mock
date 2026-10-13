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
    private volatile boolean shuttingDown = false;
    private volatile boolean readOnly = false;
    private volatile UserLocationTracker userLocationTracker = null;
    private volatile long luaTimeLimitMillis = DEFAULT_LUA_TIME_LIMIT_MILLIS;

    /**
     * Mark the start of a script run, recording the start time and clearing any
     * stale kill request left over from a previous script/function.
     */
    public void start() {
        start(false);
    }

    /**
     * Mark the start of a script run.
     *
     * @param readOnly whether the script runs read-only ({@code EVAL_RO} /
     * {@code EVALSHA_RO}): dispatching a write command is then refused
     */
    public void start(boolean readOnly) {
        start(System.nanoTime(), ScriptType.SCRIPT, null, readOnly);
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
        startFunction(invokingFunction, invokingOperation, invokingParams, false);
    }

    /**
     * Mark the start of a function run.
     *
     * @param invokingFunction the name of the library function being invoked
     * @param invokingOperation the name of the Redis operation that invoked the function (e.g. "FCALL_RO")
     * @param invokingParams the parameters passed to the Redis operation
     * @param readOnly whether the function runs read-only, because it is flagged
     * {@code no-writes} or was invoked through {@code FCALL_RO}: dispatching a
     * write command is then refused
     */
    public void startFunction(String invokingFunction, String invokingOperation, List<Slice> invokingParams,
                              boolean readOnly) {
        long startNanos = System.nanoTime();
        RunningFunctionInfo functionInfo = new RunningFunctionInfo(invokingFunction, invokingOperation, invokingParams, startNanos);
        start(startNanos, ScriptType.FUNCTION, functionInfo, readOnly);
    }

    private synchronized void start(long startNanos, ScriptType scriptType, RunningFunctionInfo functionInfo,
                                    boolean readOnly) {
        //While the server is shutting down a script must not get going: it is
        //killed at its very first instruction.
        killRequested = shuttingDown;
        writeDirty = false;
        this.readOnly = readOnly;
        userLocationTracker = null;
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
        readOnly = false;
        userLocationTracker = null;
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
     * Terminate the running script unconditionally, the way {@code SHUTDOWN
     * NOSAVE} does, and keep killing: until {@link #endShutdown()} any script
     * that starts is killed at its first instruction, so a command that was
     * already waiting for the data lock when the server stopped cannot leave a
     * script running (its {@code start()} would otherwise clear the kill).
     * Unlike {@link #requestKill(ScriptType)} this ignores whether the script
     * has written; a write dispatched after this point is refused by
     * {@link #markWrite()}, as after any granted kill.
     */
    public synchronized void beginShutdown() {
        shuttingDown = true;
        if (running) {
            killRequested = true;
        }
    }

    /**
     * Let scripts run again, once the server is (re)started.
     */
    public synchronized void endShutdown() {
        shuttingDown = false;
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
     * @return whether the running script may not write: it was started by one
     * of the {@code _RO} commands or is a {@code no-writes} function. The
     * script-side command dispatch refuses write commands while this holds.
     * Always {@code false} when nothing is running.
     */
    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * Register where the running script executes user code. Called by the Lua
     * debug hook when it enters a user chunk, so the hook that actually runs
     * the user code (there is one per Lua environment) is the one consulted.
     */
    public void setUserLocationTracker(UserLocationTracker tracker) {
        userLocationTracker = tracker;
    }

    /**
     * @return the {@code source:line} of the user-script instruction the
     * running script executed most recently, e.g. {@code @user_function:3},
     * or {@code null} if no user code has run. Error replies use it when the
     * raised error carries no location of its own, which is the case when the
     * failing {@code redis.call} was a Lua tail call: luaj has already dropped
     * the user frame by the time the command runs.
     */
    public String lastUserLocation() {
        UserLocationTracker tracker = userLocationTracker;
        return tracker == null ? null : tracker.lastUserLocation();
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

    /** See {@link #setUserLocationTracker(UserLocationTracker)}. */
    public interface UserLocationTracker {
        /** @return see {@link ScriptingManager#lastUserLocation()} */
        String lastUserLocation();
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
