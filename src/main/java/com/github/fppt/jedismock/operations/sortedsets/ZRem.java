package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("zrem")
class ZRem extends AbstractRedisOperation {

    ZRem(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        Slice key = params().get(0);
        final RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);
        if (mapDBObj.isEmpty()) {
            return Response.integer(0);
        }
        int count = 0;
        for (int i = 1; i < params().size(); i++) {
            if (mapDBObj.remove(params().get(i).toString())) {
                count++;
            }
        }
        return Response.integer(count);
    }
}
