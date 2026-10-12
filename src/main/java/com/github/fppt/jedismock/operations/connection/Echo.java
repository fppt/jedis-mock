package com.github.fppt.jedismock.operations.connection;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand(value = "echo", arity = 2)
class Echo extends AbstractRedisOperation {
    Echo(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        return Response.bulkString(params().get(0));
    }
}
