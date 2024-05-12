package com.github.fppt.jedismock.operations.hashes;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("hexists")
public class HExists extends AbstractRedisOperation {
    HExists(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        if (base().getSlice(params().get(0), params().get(1)) == null){
            return Response.integer(0);
        }
        return Response.integer(1);
    }
}
