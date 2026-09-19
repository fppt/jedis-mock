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
 * the script still holds that lock, so the remaining methods are deliberately
 * lock-free and rely on {@code volatile} visibility.
 * <p>
 * The single-writer invariant (only the running-script thread calls
 * {@code start}/{@code stop}) means {@code running} and {@code startNanos} can be
 * plain volatiles: a reader that observes {@code running == true} is guaranteed
 * by the volatile happens-before to also observe the matching {@code startNanos}.
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

    private void start(long startNanos, ScriptType scriptType, RunningFunctionInfo functionInfo) {
        killRequested = false;
        this.startNanos = startNanos;
        running = true;
        this.scriptType = scriptType;
        this.functionInfo = functionInfo;
    }

    /**
     * Mark the end of a script run (normal completion, error, or kill).
     */
    public void stop() {
        killRequested = false;
        running = false;
        scriptType = null;
        functionInfo = null;
    }

    /**
     * Request termination of the currently running script.
     *
     * @return {@code true} if a script was running, so a kill has been scheduled
     * and the caller should reply {@code OK}; {@code false} if nothing is
     * running, so the caller should reply {@code -NOTBUSY}.
     */
    public boolean requestKill() {
        if (!running) {
            return false;
        }
        killRequested = true;
        return true;
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
}
