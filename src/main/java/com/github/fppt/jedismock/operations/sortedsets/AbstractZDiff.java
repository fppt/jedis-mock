package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;

abstract class AbstractZDiff extends ZStore {

    AbstractZDiff(OperationExecutorState state, List<Slice> params) {
        super(state, params);
    }

    protected RMZSet getResult(RMZSet zset1, RMZSet zset2, double weight) {
        RMZSet result = new RMZSet();
        for (ZSetEntry entry: zset1.entries(false)) {
            result.put(entry.getValue(), entry.getScore());
        }

        for (ZSetEntry entry: zset2.entries(false)) {
            Slice value = entry.getValue();
            if (result.hasMember(value)) {
                result.remove(value);
            }
        }

        return result;
    }
}
