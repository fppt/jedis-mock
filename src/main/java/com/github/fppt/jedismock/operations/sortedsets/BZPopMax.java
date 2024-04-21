package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;

import static com.github.fppt.jedismock.Utils.toNanoTimeout;

@RedisCommand("bzpopmax")
public class BZPopMax extends BZPop {

    BZPopMax(OperationExecutorState state, List<Slice> params) {
        super(state, params);
        timeoutNanos = toNanoTimeout(params().get(params().size() - 1).toString());
    }

    @Override
    protected Slice popper(List<Slice> params) {
        List<Slice> result = new ZPop(base(), params, true).pop();
        return Response.array(Response.bulkString(params.get(0)), result.get(0), result.get(1));
    }
}
