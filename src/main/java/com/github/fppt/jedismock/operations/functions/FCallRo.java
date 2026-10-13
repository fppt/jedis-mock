package com.github.fppt.jedismock.operations.functions;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

/**
 * {@code FCALL_RO}: like {@code FCALL}, but only a function registered with
 * the {@code no-writes} flag may be invoked, and it runs read-only.
 */
@RedisCommand(value = "fcall_ro", arity = -3)
public class FCallRo extends FCall {

    public FCallRo(RedisBase base, List<Slice> params, OperationExecutorState state) {
        super(base, params, state);
    }

    @Override
    protected boolean readOnly() {
        return true;
    }
}
