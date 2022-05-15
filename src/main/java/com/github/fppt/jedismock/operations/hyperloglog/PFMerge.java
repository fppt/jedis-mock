package com.github.fppt.jedismock.operations.hyperloglog;

import com.github.fppt.jedismock.datastructures.RMHyperLogLog;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("pfmerge")
class PFMerge extends AbstractRedisOperation {
    PFMerge(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        Slice key = params().get(0);
        RMHyperLogLog rmData = base().getHLL(key);
        RMHyperLogLog set = (rmData == null ? new RMHyperLogLog() : rmData);

        for (Slice v : params().subList(1, params().size())) {
            RMHyperLogLog valueToMerge = base().getHLL(v);

            if (valueToMerge != null) {
                set.addAll(valueToMerge.getStoredData());
            }
        }

        base().putValue(key, set);
        return Response.OK;
    }
}
