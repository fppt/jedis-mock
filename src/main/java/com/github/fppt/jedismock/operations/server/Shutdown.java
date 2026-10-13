package com.github.fppt.jedismock.operations.server;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;
import java.util.Locale;

/**
 * {@code SHUTDOWN [NOSAVE | SAVE] [NOW] [FORCE] [ABORT]}. There is nothing to
 * persist, so the options matter only for validation and for the one rule they
 * drive: a shutdown that would save is refused with {@code -BUSY} while a
 * script is timing out, whereas {@code SHUTDOWN NOSAVE} terminates even a
 * script that has already written (which {@code SCRIPT KILL} refuses as
 * {@code UNKILLABLE}). A successful shutdown stops the whole
 * {@link com.github.fppt.jedismock.RedisServer}: every connection is closed
 * without a reply and the port is released.
 * <p>
 * Dispatched by {@link MockExecutor} without the data lock, like
 * {@code SCRIPT KILL}, since it must be served while a runaway script holds
 * that lock.
 */
@RedisCommand(value = "shutdown", arity = -1, transactional = false)
public class Shutdown extends AbstractRedisOperation {

    private final OperationExecutorState state;

    Shutdown(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        this.state = state;
    }

    @Override
    protected Slice response() {
        boolean nosave = false;
        boolean save = false;
        boolean now = false;
        boolean force = false;
        boolean abort = false;
        for (Slice param : params()) {
            switch (param.toString().toLowerCase(Locale.ROOT)) {
                case "nosave":
                    nosave = true;
                    break;
                case "save":
                    save = true;
                    break;
                case "now":
                    now = true;
                    break;
                case "force":
                    force = true;
                    break;
                case "abort":
                    abort = true;
                    break;
                default:
                    return Response.error("ERR syntax error");
            }
        }
        if (abort && (nosave || save || now || force) || nosave && save) {
            return Response.error("ERR syntax error");
        }
        if (abort) {
            //A shutdown here is immediate, so there is never one to abort.
            return Response.error("ERR No shutdown in progress.");
        }
        if (!nosave) {
            //Real Redis serves this only after a running script finished, unless
            //the script has already timed out: then only SHUTDOWN NOSAVE is accepted.
            Slice busy = MockExecutor.awaitScriptOrBusy(state, "shutdown");
            if (busy != null) {
                return busy;
            }
        }
        //Stops the server, which closes this connection too (no reply is ever
        //sent) and terminates the running script whether or not it has written.
        state.owner().shutdownServer();
        return Response.SKIP;
    }
}
