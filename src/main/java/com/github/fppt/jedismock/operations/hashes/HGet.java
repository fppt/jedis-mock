package com.github.fppt.jedismock.operations.hashes;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("hget")
class HGet extends AbstractRedisOperation {
    HGet(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        return Response.bulkString(base().getSlice(params().get(0), params().get(1)));
    }
}
