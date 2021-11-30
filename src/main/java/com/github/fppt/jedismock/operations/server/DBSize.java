package com.github.fppt.jedismock.operations.server;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("dbsize")
class DBSize extends AbstractRedisOperation {
    DBSize(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        return Response.integer(base().keys().size());
    }
}
