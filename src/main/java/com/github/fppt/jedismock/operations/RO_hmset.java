package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

public class RO_hmset extends RO_hset {
    public RO_hmset(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    Slice response() {
        super.response();

        return Response.OK;
    }
}
