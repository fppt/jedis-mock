package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;


public class RO_hexists extends AbstractRedisOperation {
    RO_hexists(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        if (base().getSlice(params().get(0), params().get(1)) == null){
            return Response.integer(0);
        }
        return Response.integer(1);
    }
}