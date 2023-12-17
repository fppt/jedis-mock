package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("zremrangebylex")
class ZRemRangeByLex extends AbstractZRangeByLex {

    ZRemRangeByLex(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        expectNoOptions();
        key = params().get(0);
        mapDBObj = getZSetFromBaseOrCreateEmpty(key);

        final Slice start = params().get(1);
        final Slice end = params().get(2);
        return remRangeFromKey(getRange(getStartBound(start), getEndBound(end)));
    }
}
