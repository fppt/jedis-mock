package com.github.fppt.jedismock.operations.server;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.CommandFactory;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.RedisOperationExecutor;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MockExecutor {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RedisOperationExecutor.class);

    /**
     * Proceed with execution, mocking the Redis behaviour.
     * @param state Executor state, holding the shared database and connection-specific state.
     * @param name Command name.
     * @param commandParams Command parameters.
     */
    public static Slice proceed(OperationExecutorState state, String name, List<Slice> commandParams) {

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
            } catch (Exception e) {
                LOG.error("Malformed request", e);
                state.errorTransaction();
                return Response.error(e.getMessage());
            }
        }
    }

    /**
     * Break the connection (imitate Redis shutdown).
     * @param state  state Executor state
     */
    public static Slice breakConnection(OperationExecutorState state) {
        state.owner().close();
        return Response.SKIP;
    }

}
