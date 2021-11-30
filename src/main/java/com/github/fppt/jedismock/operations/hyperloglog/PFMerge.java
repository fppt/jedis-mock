package com.github.fppt.jedismock.operations.hyperloglog;

import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RedisCommand("pfmerge")
class PFMerge extends AbstractRedisOperation {
    PFMerge(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        Slice key = params().get(0);
        RMSet rmData = base().getSet(key);
        boolean first;
        Set<Slice> set;
        if (rmData == null) {
            set = new HashSet<>();
            first = true;
        } else {
            set = rmData.getStoredData();
            first = false;
        }

        for (Slice v : params().subList(1, params().size())) {
            RMSet valueToMerge = base().getSet(v);
            if (valueToMerge != null) {
                Set<Slice> s = valueToMerge.getStoredData();
                set.addAll(s);
            }
        }

        if (first) {
            base().putValue(key, new RMSet(set));
        }
        return Response.OK;
    }
}
