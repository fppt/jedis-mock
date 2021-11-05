package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.github.fppt.jedismock.Utils.deserializeObject;
import static com.github.fppt.jedismock.Utils.serializeObject;

@RedisCommand("pfmerge")
class RO_pfmerge extends AbstractRedisOperation {
    RO_pfmerge(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        Slice key = params().get(0);
        Slice data = base().getSlice(key);
        boolean first;

        Set<Slice> set;
        if (data == null) {
            set = new HashSet<>();
            first = true;
        } else {
            set = deserializeObject(data);
            first = false;
        }
        for (Slice v : params().subList(1, params().size())) {
            Slice src = base().getSlice(v);
            if (src != null) {
                Set<Slice> s = deserializeObject(src);
                set.addAll(s);
            }
        }

        Slice out = serializeObject(set);
        if (first) {
            base().putSlice(key, out);
        } else {
            base().putSlice(key, out, null);
        }
        return Response.OK;
    }
}
