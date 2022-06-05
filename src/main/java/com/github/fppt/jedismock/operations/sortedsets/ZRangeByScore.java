package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("zrangebyscore")
public class ZRangeByScore extends AbstractByScoreOperation {
    private final Slice start;
    private final Slice end;

    public ZRangeByScore(RedisBase base, List<Slice> params) {
        super(base, params);
        start = params().get(1);
        end = params().get(2);
    }

    @Override
    protected Slice response() {
        return rangeByScore(start, end, false);
    }
}
