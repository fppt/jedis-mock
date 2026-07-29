package com.github.fppt.jedismock.server;

import com.github.fppt.jedismock.commands.RedisCommand;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;

/**
 * Created by Xiaolu on 2015/4/20.
 */
public class RedisOperationExecutor {
    private final OperationExecutorState state;

    public RedisOperationExecutor(OperationExecutorState state) {
        this.state = state;
    }

    public Slice execCommand(RedisCommand command) {
        if (command.parameters().isEmpty()) {
            throw new IllegalStateException();
        }
        List<Slice> params = command.parameters();
        List<Slice> commandParams = params.subList(1, params.size());
        String name = new String(params.get(0).data()).toLowerCase();
        Slice response = state.owner().options().getCommandInterceptor()
                .execCommand(state, name, commandParams);
        // CLIENT REPLY applies to the socket reply path only: replies produced
        // inside Lua or MULTI go through MockExecutor.proceed directly and are
        // part of a larger reply, which is never suppressed piecemeal.
        return state.applyReplyMode(response);
    }

    /**
     * Releases the server-side state of a disconnected client (its pub/sub
     * subscriptions). Deliberately does not take the shared data lock: a client
     * may disconnect while another connection runs a long Lua script that holds
     * it, and the disconnect must not wait for that script to finish. The
     * registry guards itself instead.
     */
    public void cleanup() {
        state.subscriptionRegistry().removeClient(state.owner());
    }
}
