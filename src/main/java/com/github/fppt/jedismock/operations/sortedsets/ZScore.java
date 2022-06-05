package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("zscore")
class ZScore extends AbstractRedisOperation {

    ZScore(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        Slice val = params().get(1);
        if (val == null || val.toString().isEmpty()) {
            return Response.error("Valid parameter must be provided");
        }

        final RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);

        Double score = mapDBObj.getScore(val.toString());

        return score == null ? Response.NULL : Response.bulkString(Slice.create(score.toString()));
    }
}
