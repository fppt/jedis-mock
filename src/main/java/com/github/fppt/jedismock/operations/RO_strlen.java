package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

class RO_strlen extends AbstractRedisOperation {
    RO_strlen(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        Slice value = base().getSlice(params().get(0));
        if (value == null) {
            return Response.integer(0);
        }
        return Response.integer(value.length());
    }
}
