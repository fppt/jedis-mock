package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;
import java.util.List;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("persist")
class RO_persist extends AbstractRedisOperation {
    RO_persist(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        return Response.integer(base().setDeadline(params().get(0), -1));
    }
}