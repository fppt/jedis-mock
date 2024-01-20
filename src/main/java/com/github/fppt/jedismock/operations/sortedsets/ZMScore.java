package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;

@RedisCommand("zmscore")
class ZMScore extends AbstractRedisOperation {

    ZMScore(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int minArgs() {
        return 2;
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);

        final RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);
        List<Slice> result = new ArrayList<>();

        for (int i = 1; i < params().size(); i++) {
            Double score = mapDBObj.getScore(params().get(i));
            result.add(score == null ? Response.NULL : Response.bulkString(Slice.create(String.valueOf(Math.round(score)))));
        }
        return Response.array(result);
    }
}
