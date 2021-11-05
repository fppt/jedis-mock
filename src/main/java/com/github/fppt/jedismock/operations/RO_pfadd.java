package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.github.fppt.jedismock.Utils.deserializeObject;
import static com.github.fppt.jedismock.Utils.serializeObject;

@RedisCommand("pfadd")
class RO_pfadd extends AbstractRedisOperation {
    RO_pfadd(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response(){
        Slice key = params().get(0);
        Slice data = base().getSlice(key);
        boolean first;

        Set<Slice> set;
        int prev;
        if (data == null) {
            set = new HashSet<>();
            first = true;
            prev = 0;
        } else {
            set = deserializeObject(data);
            first = false;
            prev = set.size();
        }

        for (Slice v : params().subList(1, params().size())) {
            set.add(v);
        }

        Slice out = serializeObject(set);
        if (first) {
            base().putSlice(key, out);
        } else {
            base().putSlice(key, out, null);
        }

        if (prev != set.size()) {
            return Response.integer(1L);
        }
        return Response.integer(0L);
    }
}
