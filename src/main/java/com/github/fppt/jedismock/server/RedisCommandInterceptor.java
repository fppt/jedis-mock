package com.github.fppt.jedismock.server;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;

@FunctionalInterface
public interface RedisCommandInterceptor {
    /**
     * This method is called on operation execution in JedisMock.
     *
     * You can either use it for overriding the default behaviour, or for checking the fact that specific
     * command is set to Redis.
     *
     * WARNING: if you are going to mutate state, synchronize on state.lock() first!
     * (see com.github.fppt.jedismock.operations.server.MockExecutor#proceed)
     *
     * @param state Executor state, which includes shared database and connection-specific state.
     * @param name Operation name.
     * @param params Operation parameters.
     *
     */
    Slice execCommand(OperationExecutorState state, String name, List<Slice> params);

}
