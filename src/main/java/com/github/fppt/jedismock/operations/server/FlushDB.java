package com.github.fppt.jedismock.operations.server;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("flushdb")
class FlushDB extends AbstractRedisOperation {
    FlushDB(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response(){
        base().clear();
        return Response.OK;
    }
}
