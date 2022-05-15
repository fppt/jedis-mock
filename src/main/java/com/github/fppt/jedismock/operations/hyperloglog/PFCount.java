package com.github.fppt.jedismock.operations.hyperloglog;

import com.github.fppt.jedismock.datastructures.RMHyperLogLog;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RedisCommand("pfcount")
class PFCount extends AbstractRedisOperation {
    PFCount(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        Set<Slice> set = new HashSet<>();
        for (Slice key : params()) {
            RMHyperLogLog data = base().getHLL(key);
            if (data == null) {
                continue;
            }

            Set<Slice> s = data.getStoredData();
            set.addAll(s);
        }
        return Response.integer(set.size());
    }
}
