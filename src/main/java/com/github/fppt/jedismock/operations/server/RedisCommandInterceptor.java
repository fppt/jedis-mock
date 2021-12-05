package com.github.fppt.jedismock.operations.server;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import java.util.List;

@FunctionalInterface
public interface RedisCommandInterceptor {
    Slice execCommand(OperationExecutorState state, String name, List<Slice> params);
}
