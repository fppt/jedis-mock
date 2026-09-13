package com.github.fppt.jedismock.operations.connection;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.ArrayList;
import java.util.List;

@RedisCommand(value = "hello", transactional = false)
public class Hello implements RedisOperation {

    private static final long RESP2 = 2L;
    private static final long RESP3 = 3L;

    /**
     * The server version reported in the handshake. Clients only surface this
     * value (Jedis and Lettuce both merely store it), but it has to look like a
     * Redis version, so it tracks the release JedisMock is modelled on.
     */
    private static final String VERSION = "8.0.0";

    private final OperationExecutorState state;
    private final List<Slice> params;

    Hello(OperationExecutorState state, List<Slice> params) {
        this.state = state;
        this.params = params;
    }

    @Override
    public Slice execute() {
        if (!params.isEmpty()) {
            long protover;
            try {
                protover = Long.parseLong(params.get(0).toString());
            } catch (NumberFormatException e) {
                return Response.error("ERR Protocol version is not an integer or out of range");
            }
            if (protover == RESP3) {
                //Real Redis speaks RESP3 here; JedisMock does not, and clients
                //(Jedis, Lettuce) treat a -NOPROTO reply as "fall back to RESP2".
                return Response.error("NOPROTO Resp3 not supported by JedisMock");
            }
            if (protover != RESP2) {
                return Response.error("NOPROTO unsupported protocol version");
            }
            Slice optionError = applyOptions();
            if (optionError != null) {
                return optionError;
            }
        }
        return handshake();
    }

    /**
     * Applies the AUTH and SETNAME options following the protocol version. Real
     * Redis validates every option before applying any of them, so a trailing
     * syntax error leaves the connection name untouched.
     *
     * @return an error reply, or {@code null} when every option was accepted.
     */
    private Slice applyOptions() {
        String clientName = null;
        for (int i = 1; i < params.size(); i++) {
            final String option = params.get(i).toString();
            if ("auth".equalsIgnoreCase(option) && i + 2 < params.size()) {
                //No password is modelled (see Auth), so any credentials are accepted,
                //exactly like the nopass default user of a real server.
                i += 2;
            } else if ("setname".equalsIgnoreCase(option) && i + 1 < params.size()) {
                clientName = params.get(++i).toString();
            } else {
                //Real Redis echoes the option back exactly as the client spelled it.
                return Response.error(String.format("ERR Syntax error in HELLO option '%s'", option));
            }
        }
        if (clientName != null) {
            state.setClientName(clientName);
        }
        return null;
    }

    private Slice handshake() {
        List<Slice> reply = new ArrayList<>();
        addField(reply, "server", Response.bulkString(Slice.create("redis")));
        addField(reply, "version", Response.bulkString(Slice.create(VERSION)));
        addField(reply, "proto", Response.integer(RESP2));
        addField(reply, "id", Response.integer(state.owner().getClientId()));
        addField(reply, "mode", Response.bulkString(Slice.create(
                state.owner().options().isClusterModeEnabled() ? "cluster" : "standalone")));
        addField(reply, "role", Response.bulkString(Slice.create("master")));
        addField(reply, "modules", Response.EMPTY_ARRAY);
        return Response.array(reply);
    }

    private static void addField(List<Slice> reply, String name, Slice value) {
        reply.add(Response.bulkString(Slice.create(name)));
        reply.add(value);
    }
}
