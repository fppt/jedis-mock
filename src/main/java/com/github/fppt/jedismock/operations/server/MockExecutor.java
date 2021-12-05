package com.github.fppt.jedismock.operations.server;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.CommandFactory;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.operations.connection.Quit;
import com.github.fppt.jedismock.server.RedisOperationExecutor;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MockExecutor {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RedisOperationExecutor.class);

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

    public static Slice quit(OperationExecutorState state) {
        synchronized (state.lock()) {
            try {
                RedisOperation operation = new Quit(state);
                return operation.execute();
            } catch (Exception e) {
                LOG.error("Can't quit", e);
                return Response.error(e.getMessage());
            }
        }
    }

}
