package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

import static com.github.fppt.jedismock.Utils.deserializeObject;

class RO_pfcount extends AbstractRedisOperation {
    RO_pfcount(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        Set<Slice> set = Sets.newHashSet();
        for (Slice key : params()) {
            Slice data = base().getSlice(key);
            if (data == null) {
                continue;
            }

            Set<Slice> s = deserializeObject(data);
            set.addAll(s);
        }
        return Response.integer((long) set.size());
    }
}
