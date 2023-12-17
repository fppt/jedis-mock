package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.ArgumentException;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

import static com.github.fppt.jedismock.operations.sortedsets.AbstractZRange.Options.BYLEX;
import static com.github.fppt.jedismock.operations.sortedsets.AbstractZRange.Options.REV;

@RedisCommand("zrangebyscore")
public class ZRangeByScore extends AbstractZRangeByScore {

    public ZRangeByScore(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        if (options.contains(BYLEX) || options.contains(REV)) {
            throw new ArgumentException("*syntax*");
        }
        key = params().get(0);
        mapDBObj = getZSetFromBaseOrCreateEmpty(key);

        final Slice start = params().get(1);
        final Slice end = params().get(2);
        return getSliceFromRange(getRange(getStartBound(start), getEndBound(end)));
    }
}
