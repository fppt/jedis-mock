package com.github.fppt.jedismock.operations.server;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;

@RedisCommand(value = "info", transactional = false)
class Info implements RedisOperation {
    @Override
    public Slice execute() {
        return Response.bulkString(Slice.create("Redis Mock Server Info"));
    }
}
