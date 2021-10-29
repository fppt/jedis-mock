package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;

class RO_info implements RedisOperation {
    @Override
    public Slice execute() {
        return Response.bulkString(Slice.create("Redis Mock Server Info"));
    }
}
