package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("zadd")
class ZAdd extends AbstractByScoreOperation {

    ZAdd(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        final RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);
        int count = 0;
        for (int i = 1; i < params().size(); i += 2) {
            Slice score = params().get(i);
            Slice value = params().get(i + 1);

            // Score must be a double. Will throw an exception if it's not.
            double s = toDouble(score.toString());

            Double prevScore = mapDBObj.put(value, s);
            if (prevScore == null) count++;
        }
        base().putValue(key, mapDBObj);
        return Response.integer(count);
    }

}
