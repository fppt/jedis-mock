package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.Optional;

public class CombinedOperationFactory {
    private final ReflectionOperationFactory<RedisBase> txOperationFactory =
            new ReflectionOperationFactory<>(TxOperation.class);
    private final ReflectionOperationFactory<OperationExecutorState> metaOperationFactory =
            new ReflectionOperationFactory<>(MetaOperation.class);

    public RedisOperation buildTxOperation(String name, RedisBase base, List<Slice> params) {
        Optional<RedisOperation> operation = txOperationFactory.buildOperation(name, base, params);
        if (!operation.isPresent()) {
            throw new UnsupportedOperationException(String.format("Unsupported operation '%s'", name));
        }
        return operation.get();
    }

    public Optional<RedisOperation> buildMetaOperation(String name, OperationExecutorState state, List<Slice> params) {
        return metaOperationFactory.buildOperation(name, state, params);
    }
}
