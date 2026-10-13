package com.github.fppt.jedismock.operations.scripting;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

/**
 * {@code EVAL_RO}: like {@code EVAL}, but any write command the script
 * dispatches is refused with {@code ERR Write commands are not allowed from
 * read-only scripts.}. The script is still added to the script cache.
 */
@RedisCommand(value = "eval_ro", arity = -3)
public class EvalRo extends Eval {

    public EvalRo(final RedisBase base, final List<Slice> params, final OperationExecutorState state) {
        super(base, params, state);
    }

    @Override
    public Slice response() {
        return response(true);
    }
}
