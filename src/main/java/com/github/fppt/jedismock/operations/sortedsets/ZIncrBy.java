package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("zincrby")
public class ZIncrBy extends AbstractByScoreOperation {
    public ZIncrBy(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int minArgs() {
        return 3;
    }

    @Override
    protected int maxArgs() {
        return 3;
    }

    @Override
    protected Slice response() {
        return Response.bulkString(Slice.create(String.valueOf(getNewScore())));
    }

    protected double getNewScore() {
        Slice key = params().get(0);
        String increment = params().get(1).toString();
        Slice member = params().get(2);
        final RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);
        double score = (mapDBObj.getScore(member) == null) ? 0d :
                mapDBObj.getScore(member);

        double newScore = getSum(score, increment);

        mapDBObj.put(member, newScore);
        base().putValue(key, mapDBObj);
        return newScore;
    }

}
