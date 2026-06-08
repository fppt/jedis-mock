package com.github.fppt.jedismock.operations.server;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

/**
 * WAITAOF numlocal numreplicas timeout
 * <p>
 * The mock neither fsyncs an AOF nor has replicas, so it reports that the write
 * reached zero local AOFs and zero replica AOFs: it returns {@code [0, 0]}
 * immediately, without blocking. Like {@link Wait}, this lets the command be
 * called from a Lua script, which must never block.
 */
@RedisCommand("waitaof")
class WaitAof extends AbstractRedisOperation {
    WaitAof(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int minArgs() {
        return 3;
    }

    @Override
    protected Slice response() {
        return Response.array(Response.integer(0), Response.integer(0));
    }
}
