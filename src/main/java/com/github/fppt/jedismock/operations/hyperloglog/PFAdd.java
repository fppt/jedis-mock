package com.github.fppt.jedismock.operations.hyperloglog;

import com.github.fppt.jedismock.datastructures.RMHyperLogLog;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("pfadd")
class PFAdd extends AbstractRedisOperation {
    PFAdd(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response(){
        Slice key = params().get(0);
        RMHyperLogLog dataSet = base().getHLL(key);
        boolean first = true;

        int prev = 0;
        if (dataSet == null) {
            dataSet = new RMHyperLogLog();
        } else {
            first = false;
            prev = dataSet.size();
        }

        dataSet.addAll(params().subList(1, params().size()));

        if (first) {
            base().putValue(key, dataSet);
        } else {
            base().putValue(key, dataSet, null);
        }

        return Response.integer((prev != dataSet.size()) ? 1 : 0);
    }
}
