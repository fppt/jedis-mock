package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

abstract class AbstractZUnion extends ZStore {

    AbstractZUnion(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected RMZSet getResult(RMZSet zset1, RMZSet zset2, double weight) {
        RMZSet result = new RMZSet();
        for (ZSetEntry entry :
                zset1.entries(false)) {
            result.put(entry.getValue(), entry.getScore());
        }
        for (ZSetEntry entry :
                zset2.entries(false)) {
            if (result.hasMember(entry.getValue())) {
                result.put(entry.getValue(), aggregate.apply(result.getScore(entry.getValue()), getMultiple(entry.getScore(), weight)));
            } else {
                result.put(entry.getValue(), getMultiple(entry.getScore(), weight));
            }
        }
        return result;
    }
}
