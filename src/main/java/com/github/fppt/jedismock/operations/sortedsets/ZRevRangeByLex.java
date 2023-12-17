package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.ArgumentException;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

import static com.github.fppt.jedismock.operations.sortedsets.AbstractZRange.Options.BYSCORE;
import static com.github.fppt.jedismock.operations.sortedsets.AbstractZRange.Options.REV;
import static com.github.fppt.jedismock.operations.sortedsets.AbstractZRange.Options.WITHSCORES;

@RedisCommand("zrevrangebylex")
class ZRevRangeByLex extends AbstractZRangeByLex {

    ZRevRangeByLex(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        if (options.contains(BYSCORE) || options.contains(WITHSCORES)) {
            throw new ArgumentException("*syntax*");
        }
        key = params().get(0);
        mapDBObj = getZSetFromBaseOrCreateEmpty(key);

        final Slice start = params().get(2);
        final Slice end = params().get(1);
        options.add(REV);
        if (invalidateStart(start.toString())) {
            return buildErrorResponse("start");
        }
        if (invalidateEnd(end.toString())) {
            return buildErrorResponse("end");
        }
        return getSliceFromRange(getRange(getStartBound(start), getEndBound(end)));
    }
}
