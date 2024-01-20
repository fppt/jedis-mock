package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.ArrayList;
import java.util.List;

import static com.github.fppt.jedismock.Utils.toNanoTimeout;

@RedisCommand("bzmpop")
public class BZMPop extends BZPop {
    private Slice numKeys;

    BZMPop(OperationExecutorState state, List<Slice> params) {
        super(state, params);
    }

    @Override
    protected int minArgs() {
        return 4;
    }

    @Override
    protected void doOptionalWork(){
        timeoutNanos = toNanoTimeout(params().get(0).toString());
        params().remove(0);
        ZMPop zmPop = new ZMPop(base(), new ArrayList<>(params()));
        numKeys = params().remove(0);
        keys = zmPop.parseArgs();
        keys.remove(0);
    }

    @Override
    protected Slice popper(List<Slice> params) {
        List<Slice> newParams = new ArrayList<>(params());
        newParams.add(0, numKeys);
        ZMPop zmPop = new ZMPop(base(), newParams);
        return zmPop.response();
    }

}
