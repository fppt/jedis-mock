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

    private static final double DELTA = 1e-6;

    ZScore(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        Slice val = params().get(1);

        final RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);

        Double score = mapDBObj.getScore(val);

        if (score == null) {
            return Response.NULL;
        }
        if (score.isInfinite()) {
            return score > 0 ? Response.bulkString(Slice.create("inf"))
                    : Response.bulkString(Slice.create("-inf"));
        }
        long round = Math.round(score);
        if (Math.abs(score - round) < DELTA) {
            return Response.bulkString(Slice.create(String.format("%.0f", score)));
        }
        return Response.bulkString(Slice.create(String.format("%10.16e", score).replace(',','.')));
    }
}
