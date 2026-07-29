package com.github.fppt.jedismock.operations.connection;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;
import java.util.Locale;

@RedisCommand(value = "client", transactional = false)
public class Client implements RedisOperation {

    private final OperationExecutorState state;
    private final List<Slice> params;

    public Client(OperationExecutorState state, List<Slice> params) {
        this.state = state;
        this.params = params;
    }

    @Override
    public Slice execute() {
        if (params.isEmpty()) {
            return Response.error("wrong number of arguments for 'client' command");
        }
        final String subcommand = params.get(0).toString();
        if ("setname".equalsIgnoreCase(subcommand)) {
            if (params.size() != 2) {
                return wrongNumberOfArguments(subcommand);
            }
            state.setClientName(params.get(1).toString());
        } else if ("getname".equalsIgnoreCase(subcommand)) {
            if (params.size() != 1) {
                return wrongNumberOfArguments(subcommand);
            }
            String name = state.getClientName();
            return name == null ? Response.NULL : Response.bulkString(Slice.create(name));
        } else if ("reply".equalsIgnoreCase(subcommand)) {
            if (params.size() != 2) {
                return wrongNumberOfArguments(subcommand);
            }
            // The +OK returned below is itself subject to the mode just set:
            // OperationExecutorState.applyReplyMode suppresses it for OFF/SKIP.
            final String mode = params.get(1).toString();
            if ("on".equalsIgnoreCase(mode)) {
                state.replyOn();
            } else if ("off".equalsIgnoreCase(mode)) {
                state.replyOff();
            } else if ("skip".equalsIgnoreCase(mode)) {
                state.replySkip();
            } else {
                return Response.error("ERR syntax error");
            }
        }
        //Unknown subcommands are accepted as no-ops rather than rejected the way
        //real Redis does: clients send informational ones we do not model (for
        //instance Lettuce's CLIENT SETINFO and CLIENT MAINT_NOTIFICATIONS on
        //connect), and failing those would break the connection handshake.
        return Response.clientResponse("client", Response.OK);
    }

    private static Slice wrongNumberOfArguments(String subcommand) {
        return Response.error(String.format("ERR wrong number of arguments for 'client|%s' command",
                subcommand.toLowerCase(Locale.ROOT)));
    }
}
