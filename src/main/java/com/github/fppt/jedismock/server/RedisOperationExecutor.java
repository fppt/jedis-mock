package com.github.fppt.jedismock.server;

import com.github.fppt.jedismock.commands.RedisCommand;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.CommandFactory;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by Xiaolu on 2015/4/20.
 */
public class RedisOperationExecutor {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RedisOperationExecutor.class);
    private final OperationExecutorState state;
    public RedisOperationExecutor(OperationExecutorState state) {
        this.state = state;
    }

    public Slice execCommand(RedisCommand command) {
        if (!(command.parameters().size() > 0)) {
            throw new IllegalStateException();
        }
        List<Slice> params = command.parameters();
        List<Slice> commandParams = params.subList(1, params.size());
        String name = new String(params.get(0).data()).toLowerCase();

        //No parallel execution
        synchronized (state.lock()){
            try {
                //Checking if we are affecting the server or client state.
                //This is done outside the context of a transaction which is why it's a separate check
                RedisOperation operation = CommandFactory.buildOperation(name, false, state, commandParams);
                if (operation != null) {
                    return operation.execute();
                }

                //Checking if we mutating the transaction or the redisBases
                operation = CommandFactory.buildOperation(name, true, state, commandParams);
                if (operation != null) {
                    if (state.isTransactionModeOn()) {
                        state.tx().add(operation);
                    } else {
                        return Response.clientResponse(name, operation.execute());
                    }
                    return Response.clientResponse(name, Response.OK);
                } else {
                    return Response.error(String.format("Unsupported operation: %s", name));
                }
            } catch (Exception e) {
                LOG.error("Malformed request", e);
                return Response.error(e.getMessage());
            }
        }
    }
}
