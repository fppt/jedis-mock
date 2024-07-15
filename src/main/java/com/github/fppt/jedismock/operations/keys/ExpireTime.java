package com.github.fppt.jedismock.operations.keys;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("expiretime")
class ExpireTime extends AbstractRedisOperation {
    ExpireTime(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        Slice key = params().get(0);
        if (!base().exists(key)) {
            return Response.integer(-2L);
        }
        Long deadline = base().getDeadline(params().get(0));
        if (deadline == null || deadline < 0) {
            return Response.integer(-1L);
        }
        return Response.integer(deadline / 1000);
    }
}
