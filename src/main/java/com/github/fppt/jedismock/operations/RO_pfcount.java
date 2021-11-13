package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RedisCommand("pfcount")
class RO_pfcount extends AbstractRedisOperation {
    RO_pfcount(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        Set<Slice> set = new HashSet<>();
        for (Slice key : params()) {
            RMSet data = base().getSet(key);
            if (data == null) {
                continue;
            }

            Set<Slice> s = data.getStoredData();
            set.addAll(s);
        }
        return Response.integer((long) set.size());
    }
}
