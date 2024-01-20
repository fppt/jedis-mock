package com.github.fppt.jedismock.operations.streams;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

/**
 * XLEN key
 */
@RedisCommand("xlen")
public class XLen extends AbstractRedisOperation {
    XLen(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int minArgs() {
        return 1;
    }

    @Override
    protected int maxArgs() {
        return 1;
    }

    @Override
    protected Slice response() {
        return Response.integer(getStreamFromBaseOrCreateEmpty(params().get(0)).getStoredData().size());
    }
}
