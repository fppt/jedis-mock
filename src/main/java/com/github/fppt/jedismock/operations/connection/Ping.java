package com.github.fppt.jedismock.operations.connection;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("ping")
class Ping extends AbstractRedisOperation {
    Ping(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        if (params().isEmpty()){
            return Response.bulkString(Slice.create("PONG"));
        }

        return Response.bulkString(params().get(0));
    }
}
