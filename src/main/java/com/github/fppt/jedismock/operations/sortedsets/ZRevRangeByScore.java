package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("zrevrangebyscore")
public class ZRevRangeByScore extends AbstractByScoreOperation {
    private final Slice start;
    private final Slice end;


    ZRevRangeByScore(RedisBase base, List<Slice> params) {
        super(base, params);
        //NB: reverse order of arguments, cf. ZRangeByScore
        start = params().get(2);
        end = params().get(1);
    }

    @Override
    protected Slice response() {
        return rangeByScore(start, end, true);
    }
}
