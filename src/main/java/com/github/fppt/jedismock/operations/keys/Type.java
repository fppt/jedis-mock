package com.github.fppt.jedismock.operations.keys;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("type")
class Type extends AbstractRedisOperation {
    Type(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        return Response.bulkString(base().type(params().get(0)));
    }
}
