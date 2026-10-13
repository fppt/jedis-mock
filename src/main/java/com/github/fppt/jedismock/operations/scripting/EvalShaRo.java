package com.github.fppt.jedismock.operations.scripting;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

/**
 * {@code EVALSHA_RO}: like {@code EVALSHA}, but the cached script runs in
 * read-only mode, as with {@link EvalRo}.
 */
@RedisCommand(value = "evalsha_ro", arity = -3)
public class EvalShaRo extends EvalSha {

    public EvalShaRo(final RedisBase base, final List<Slice> params, final OperationExecutorState state) {
        super(base, params, state);
    }

    @Override
    protected boolean readOnly() {
        return true;
    }
}
