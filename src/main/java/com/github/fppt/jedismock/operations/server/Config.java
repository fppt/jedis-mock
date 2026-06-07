package com.github.fppt.jedismock.operations.server;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.ArrayList;
import java.util.List;

/**
 * Thin {@code CONFIG GET}/{@code CONFIG SET} support. The aim is not to model
 * real Redis configuration but to let code that issues {@code CONFIG} work
 * (e.g. Spring Data Redis / Spring Session, which send
 * {@code CONFIG SET notify-keyspace-events} automatically when a key-expiration
 * listener is registered) and to let a value written with {@code SET} be read
 * back with {@code GET}.
 * <p>
 * Most parameters are simply stored verbatim in a server-wide namespace
 * ({@link OperationExecutorState#configuration()}) and have no effect on the
 * mock. The exceptions are the behavioural parameters that the mock actually
 * honours — {@code lua-time-limit} (alias {@code busy-reply-threshold}), routed
 * to the {@link com.github.fppt.jedismock.storage.ScriptingManager}, and
 * {@code proto-max-bulk-len}, routed to the configuration's typed accessor.
 * <p>
 * Glob-style patterns in {@code CONFIG GET} are not supported; each argument is
 * treated as a literal parameter name.
 */
@RedisCommand(value = "config", transactional = false)
class Config extends AbstractRedisOperation {
    private static final String LUA_TIME_LIMIT = "lua-time-limit";
    private static final String BUSY_REPLY_THRESHOLD = "busy-reply-threshold";
    private static final String PROTO_MAX_BULK_LEN = "proto-max-bulk-len";

    private final OperationExecutorState state;

    Config(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        this.state = state;
    }

    @Override
    protected int minArgs() {
        return 1;
    }

    @Override
    protected Slice response() {
        String subCommand = params().get(0).toString();
        if ("SET".equalsIgnoreCase(subCommand)) {
            return set();
        } else if ("GET".equalsIgnoreCase(subCommand)) {
            return get();
        }
        //RESETSTAT, REWRITE, etc.: accept as no-ops.
        return Response.OK;
    }

    private Slice set() {
        for (int i = 1; i + 1 < params().size(); i += 2) {
            String name = params().get(i).toString();
            String value = params().get(i + 1).toString();
            if (isLuaTimeLimit(name)) {
                //Behavioural parameter: route to the component that uses it.
                state.scriptingManager().setLuaTimeLimitMillis(Long.parseLong(value));
            } else if (PROTO_MAX_BULK_LEN.equalsIgnoreCase(name)) {
                state.configuration().setProtoMaxBulkLen(Long.parseLong(value));
            } else {
                //Everything else just round-trips through the thin namespace.
                state.configuration().set(name, value);
            }
        }
        return Response.OK;
    }

    private Slice get() {
        List<Slice> result = new ArrayList<>();
        for (int i = 1; i < params().size(); i++) {
            String name = params().get(i).toString();
            result.add(Response.bulkString(Slice.create(name)));
            result.add(Response.bulkString(Slice.create(valueOf(name))));
        }
        return Response.array(result);
    }

    private String valueOf(String name) {
        if (isLuaTimeLimit(name)) {
            return Long.toString(state.scriptingManager().getLuaTimeLimitMillis());
        }
        if (PROTO_MAX_BULK_LEN.equalsIgnoreCase(name)) {
            return Long.toString(state.configuration().getProtoMaxBulkLen());
        }
        return state.configuration().get(name);
    }

    private static boolean isLuaTimeLimit(String name) {
        return LUA_TIME_LIMIT.equalsIgnoreCase(name) || BUSY_REPLY_THRESHOLD.equalsIgnoreCase(name);
    }
}
