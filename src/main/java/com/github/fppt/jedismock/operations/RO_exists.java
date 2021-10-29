package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@TxOperation("exists")
class RO_exists extends AbstractRedisOperation {
    RO_exists(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        if (base().exists(params().get(0))) {
            return Response.integer(1);
        }
        return Response.integer(0);
    }
}
