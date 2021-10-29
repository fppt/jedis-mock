package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@TxOperation("setnx")
class RO_setnx extends AbstractRedisOperation {
    RO_setnx(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response(){
        if (base().getValue(params().get(0)) == null) {
            base().putValue(params().get(0), params().get(1));
            return Response.integer(1);
        }
        return Response.integer(0);
    }
}
