package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

class RO_ping extends AbstractRedisOperation {
    RO_ping(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        if (params().isEmpty()){
            return Response.bulkString(Slice.create("PONG"));
        }

        return Response.bulkString(params().get(0));
    }
}
