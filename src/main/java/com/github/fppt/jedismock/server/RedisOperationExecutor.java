package com.github.fppt.jedismock.server;

import com.github.fppt.jedismock.commands.RedisCommand;
import com.github.fppt.jedismock.exception.WrongValueTypeException;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.operations.CombinedOperationFactory;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.google.common.base.Preconditions;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * Created by Xiaolu on 2015/4/20.
 */
public class RedisOperationExecutor {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RedisOperationExecutor.class);
    private final OperationExecutorState state;
    private final CombinedOperationFactory operationFactory = new CombinedOperationFactory();

    public RedisOperationExecutor(OperationExecutorState state) {
        this.state = state;
    }


    public synchronized Slice execCommand(RedisCommand command) {
        Preconditions.checkArgument(command.parameters().size() > 0);
        List<Slice> params = command.parameters();
        List<Slice> commandParams = params.subList(1, params.size());
        String name = new String(params.get(0).data()).toLowerCase();

        try {
            //Checking if we are affecting the server or client state.
            //This is done outside the context of a transaction which is why it's a separate check
            Optional<RedisOperation> result = operationFactory.buildMetaOperation(name, state, commandParams);
            if (result.isPresent()) return result.get().execute();

            //Checking if we mutating the transaction or the redisBases
            RedisOperation redisOperation = operationFactory.buildTxOperation(name, state.base(), commandParams);
            if (state.isTransactionModeOn()) {
                state.tx().add(redisOperation);
            } else {
                return Response.clientResponse(name, redisOperation.execute());
            }

            return Response.clientResponse(name, Response.OK);
        } catch (UnsupportedOperationException | WrongValueTypeException | IllegalArgumentException e) {
            LOG.error("Malformed request", e);
            return Response.error(e.getMessage());
        }
    }
}
