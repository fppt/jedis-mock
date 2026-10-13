package com.github.fppt.jedismock.operations.keys;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand(value = "exists", arity = -2)
class Exists extends AbstractRedisOperation {
    Exists(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        int count = 0;
        for (Slice key : params()) {
            if (base().exists(key)) {
                count++;
            }
        }
        return Response.integer(count);
    }
}
