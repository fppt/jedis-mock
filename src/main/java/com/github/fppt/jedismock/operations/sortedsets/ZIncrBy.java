package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.ArgumentException;
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
    protected Slice response() {
        if (params().size() != 3) {
            throw new ArgumentException("ERR wrong number of arguments for 'zincrby' command");
        }

        Slice key = params().get(0);
        String increment = params().get(1).toString();
        Slice member = params().get(2);
        final RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);
        double score = (mapDBObj.getScore(member) == null) ? 0d :
                                                             mapDBObj.getScore(member);

        double newScore = getSum(score, increment);

        mapDBObj.put(member, newScore);
        base().putValue(key, mapDBObj);
        return Response.doubleValue(newScore);
    }

    private Double getSum(Double score, String increment) {
        if ("+inf".equalsIgnoreCase(increment)) {
            if (score == Double.NEGATIVE_INFINITY) {
                throw new ArgumentException("ERR resulting score is not a number (NaN)");
            } else {
                return Double.POSITIVE_INFINITY;
            }
        } else if ("-inf".equalsIgnoreCase(increment)) {
            if (score == Double.POSITIVE_INFINITY) {
                throw new ArgumentException("ERR resulting score is not a number (NaN)");
            } else {
                return Double.NEGATIVE_INFINITY;
            }
        } else if (score.isInfinite()) {
            return score;
        } else {
            return score + toDouble(increment);
        }
    }
}
