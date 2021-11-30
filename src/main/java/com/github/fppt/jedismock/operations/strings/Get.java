package com.github.fppt.jedismock.operations.strings;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("get")
class Get extends AbstractRedisOperation {
    Get(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        return Response.bulkString(base().getSlice(params().get(0)));
    }
}
