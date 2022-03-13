package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;

@RedisCommand("zrevrange")
class ZRevRange extends AbstractRedisOperation {

    private static final String IS_REV = "REV";
    private final ZRange zRange;


    ZRevRange(RedisBase base, List<Slice> params) {
        super(base, params);
        List<Slice> updatedParams = new ArrayList<>(params);
        updatedParams.add(Slice.create(IS_REV));
        this.zRange = new ZRange(base, updatedParams);
    }

    @Override
    protected Slice response() {
        return zRange.response();
    }
}
