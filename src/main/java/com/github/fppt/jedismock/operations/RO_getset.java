package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@TxOperation("getset")
class RO_getset extends AbstractRedisOperation {
    RO_getset(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        Slice value = base().getValue(params().get(0));
        base().putValue(params().get(0), params().get(1));
        return Response.bulkString(value);
    }
}
