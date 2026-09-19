package com.github.fppt.jedismock.operations.server;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.ArgumentException;
import com.github.fppt.jedismock.exception.WrongValueTypeException;
import com.github.fppt.jedismock.operations.CommandFactory;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.operations.functions.LibraryInfo;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.storage.ScriptingManager;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MockExecutor {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(MockExecutor.class);

    /**
     * Poll interval while waiting out a script that has not yet exceeded
     * lua-time-limit. Small enough that a fast script adds negligible latency,
     * large enough to avoid busy-spinning.
     */
    private static final long BUSY_POLL_MILLIS = 5L;

    /**
     * The replies real Redis gives to a command issued while a script/function is timing
     * out. Kept verbatim so harmonized tests (and clients matching on it) behave
     * identically to a real server.
     */
    private static final String BUSY_SCRIPT_MESSAGE =
            "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.";
    private static final String BUSY_FUNCTION_MESSAGE =
            "BUSY Redis is busy running a script. You can only call FUNCTION KILL or SHUTDOWN NOSAVE.";

    /**
     * Proceed with execution, mocking the Redis behaviour.
     * @param state Executor state, holding the shared database and connection-specific state.
     * @param name Command name.
     * @param commandParams Command parameters.
     */
    public static Slice proceed(OperationExecutorState state, String name, List<Slice> commandParams) {

        //FUNCTION/SCRIPT KILL must take effect while a runaway script still holds the
        //global data lock, so it is handled here, before acquiring that lock.
        //Otherwise it would block behind the very script it is meant to abort.
        if (("function".equals(name) || "script".equals(name)) && !commandParams.isEmpty()
                && "kill".equalsIgnoreCase(commandParams.get(0).toString())) {
            ScriptingManager scripting = state.scriptingManager();
            ScriptingManager.ScriptType expectedScriptType =
                    "function".equals(name) ? ScriptingManager.ScriptType.FUNCTION : ScriptingManager.ScriptType.SCRIPT;
            ScriptingManager.ScriptType actualScriptType = scripting.getRunningScriptType();
            // SCRIPT KILL cannot kill a running FUNCTION and vice versa.
            if (actualScriptType == null || actualScriptType == expectedScriptType) {
                if (!scripting.requestKill()) {
                    return Response.error("NOTBUSY No scripts in execution right now.");
                }
                //Reply OK only once the script has actually aborted, so a command
                //issued right after SCRIPT KILL no longer sees the script as busy.
                scripting.awaitStopped();
                return Response.OK;
            }
        }

        //FUNCTION STATS is a read-only command that provides information on the
        //currently running function. It is handled here, without acquiring the
        //global data lock; otherwise it would block behind the very script it is
        //meant to return information about.
        if ("function".equals(name) && !commandParams.isEmpty()
                && "stats".equalsIgnoreCase(commandParams.get(0).toString())) {
            return functionStats(state.base(), state.scriptingManager());
        }

        //MULTI is allow-busy in real Redis and only flips this connection's
        //transaction flag (no shared data), so service it without the data lock.
        //Otherwise, while a script is busy, opening a transaction would either be
        //rejected with BUSY or block behind the running script — and then a data
        //command queued in that transaction could never get the BUSY error that
        //must dirty it (so EXEC would not abort, as real Redis requires).
        if ("multi".equals(name)) {
            return CommandFactory.buildOperation(name, false, state, commandParams).execute();
        }

        //BUSY gate. While another connection runs a Lua script we must not block
        //on the data lock (we could never wake to reply); instead we check the
        //script's liveness *before* contending for the lock. A script that has
        //exceeded lua-time-limit makes us reply -BUSY; a younger one makes us
        //wait (polling, since a fast script finishes and we should then proceed).
        //Done on the script thread itself this would deadlock, but a script's own
        //redis.call goes through LuaRedisCallback, not here, so that can't happen.
        //
        //Note: this check is NOT atomic with acquiring the lock below. In a
        //microsecond-wide window a *different* connection's script could start
        //right after we observe "idle" here, so we would then block on the
        //monitor instead of replying BUSY. Closing that race would mean replacing
        //the intrinsic monitor with a ReentrantLock + Conditions (and migrating
        //the blocking-op wait/notify too); deemed not worth it for a mock.
        Slice busy = awaitScriptOrBusy(state, name);
        if (busy != null) {
            return busy;
        }

        synchronized (state.lock()) {
            try {
                //Checking if we are affecting the server or client state.
                //This is done outside the context of a transaction which is why it's a separate check
                RedisOperation operation = CommandFactory.buildOperation(name, false, state, commandParams);
                if (operation != null) {
                    return operation.execute();
                }

                //Checking if we are mutating the transaction or the redisBases
                operation = CommandFactory.buildOperation(name, true, state, commandParams);
                if (operation != null) {
                    if (state.isTransactionModeOn()) {
                        state.tx().add(operation);
                        return Response.clientResponse(name, Response.QUEUED);
                    } else {
                        return Response.clientResponse(name, operation.execute());
                    }
                } else {
                    state.errorTransaction();
                    return Response.error(String.format("Unsupported operation: %s", name));
                }
            } catch (ArgumentException | IllegalArgumentException | WrongValueTypeException e) {
                // Expected guards, so we can just return the error message.
                state.errorTransaction();
                return Response.error(e.getMessage());
            } catch (RuntimeException e) {
                LOG.error("Malformed request", e);
                state.errorTransaction();
                return Response.error(e.getMessage());
            }
        }
    }

    /**
     * If another connection is running a Lua script, wait for it: return a
     * {@code -BUSY} reply once it has exceeded {@code lua-time-limit}, or
     * {@code null} once it finishes (so the caller may proceed to the lock).
     * Polls rather than blocking on the data lock, because a thread blocked on
     * the monitor could never wake up to reply BUSY.
     *
     * @return the BUSY reply Slice, or {@code null} to proceed normally.
     */
    private static Slice awaitScriptOrBusy(OperationExecutorState state, String name) {
        ScriptingManager scripting = state.scriptingManager();
        while (scripting.isRunning()) {
            if (scripting.isBusy()) {
                String busyMessage;
                switch (scripting.getRunningScriptType()) {
                    case SCRIPT:
                        busyMessage = BUSY_SCRIPT_MESSAGE;
                        break;
                    case FUNCTION:
                        busyMessage = BUSY_FUNCTION_MESSAGE;
                        break;
                    default:
                        throw new IllegalStateException("Unreachable");
                }
                if ("exec".equals(name) && state.isTransactionModeOn()) {
                    //Real Redis rejects EXEC during a busy script and, because the
                    //rejected command is EXEC, reports it as an aborted transaction
                    //that names the cause, discarding the transaction. (Mirrors
                    //the ERRORED branch of Exec, minus the generic message.)
                    state.transactionMode(false);
                    state.tx().clear();
                    return Response.error("EXECABORT Transaction discarded because of: " + busyMessage);
                }
                //A data command rejected with BUSY while being queued in MULTI
                //dirties the transaction, so a later EXEC aborts (matching real
                //Redis); errorTransaction() is a no-op outside a transaction.
                state.errorTransaction();
                return Response.error(busyMessage);
            }
            try {
                Thread.sleep(BUSY_POLL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Response.SKIP;
            }
        }
        return null;
    }

    /**
     * Break the connection (imitate Redis shutdown).
     * @param state  state Executor state
     */
    public static Slice breakConnection(OperationExecutorState state) {
        state.owner().close();
        return Response.SKIP;
    }

    private static Slice functionStats(RedisBase base, ScriptingManager scripting) {
        Map<String, LibraryInfo> libraries = base.getLuaLibraries();
        ScriptingManager.RunningFunctionInfo functionInfo = scripting.getRunningFunctionInfo();
        return Response.array(
                Response.bulkString(Slice.create("running_script")),
                functionInfo != null ?
                        Response.array(
                                Response.bulkString(Slice.create("name")),
                                Response.bulkString(Slice.create(functionInfo.getInvokingFunction())),
                                Response.bulkString(Slice.create("command")),
                                Response.array(functionInfo.getInvokingCommand().stream().map(Response::bulkString).collect(Collectors.toList())),
                                Response.bulkString(Slice.create("duration_ms")),
                                Response.integer(Duration.ofNanos(System.nanoTime() - functionInfo.getStartNanos()).toMillis())
                        )
                        : Response.NULL,

                Response.bulkString(Slice.create("engines")),
                Response.array(
                        Response.bulkString(Slice.create("LUA")),
                        Response.array(
                                Response.bulkString(Slice.create("libraries_count")),
                                Response.integer(libraries.size()),
                                Response.bulkString(Slice.create("functions_count")),
                                Response.integer(libraries.values().stream().mapToLong(libraryInfo -> libraryInfo.getFunctionNames().size()).sum())
                        )
                )
        );
    }
}
