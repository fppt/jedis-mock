package com.github.fppt.jedismock.operations.server;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

/**
 * WAIT numreplicas timeout
 * <p>
 * The mock is a standalone master with no replicas, so no write is ever
 * replicated: WAIT returns 0 (the number of replicas that acknowledged)
 * immediately, without blocking. This mirrors real Redis with no connected
 * replicas, and in particular lets it be called from a Lua script, which must
 * never block.
 */
@RedisCommand("wait")
class Wait extends AbstractRedisOperation {
    Wait(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int minArgs() {
        return 2;
    }

    @Override
    protected int maxArgs() {
        return 2;
    }

    @Override
    protected Slice response() {
        return Response.integer(0);
    }
}
