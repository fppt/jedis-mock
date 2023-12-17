package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;

@RedisCommand("zrevrank")
public class ZRevRank extends AbstractByScoreOperation {
    private static final String IS_REV = "REV";
    private final ZRank zRank;

    ZRevRank(RedisBase base, List<Slice> params) {
        super(base, params);
        List<Slice> updatedParams = new ArrayList<>(params);
        updatedParams.add(Slice.create(IS_REV));
        this.zRank = new ZRank(base, updatedParams);
    }

    @Override
    protected Slice response() {
        return zRank.response();
    }
}
