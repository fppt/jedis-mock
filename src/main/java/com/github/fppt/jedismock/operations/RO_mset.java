package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@TxOperation("mset")
class RO_mset extends AbstractRedisOperation {
    RO_mset(RedisBase base, List<Slice> params ) {
        super(base, params);
    }

    Slice response() {
        for (int i = 0; i < params().size(); i += 2) {
            base().putValue(params().get(i), params().get(i + 1));
        }
        return Response.OK;
    }
}
