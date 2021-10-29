package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@TxOperation("hget")
class RO_hget extends AbstractRedisOperation {
    RO_hget(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        return Response.bulkString(base().getValue(params().get(0), params().get(1)));
    }
}
