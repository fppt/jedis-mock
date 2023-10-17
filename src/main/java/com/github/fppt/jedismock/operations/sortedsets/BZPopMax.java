package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("bzpopmax")
public class BZPopMax extends AbstractByScoreOperation {
    private final BZPopMin bzPopMin;

    BZPopMax(RedisBase base, List<Slice> params) {
        super(base, params);
        this.bzPopMin = new BZPopMin(base, params());
        bzPopMin.setRev(true);
    }

    @Override
    protected Slice response() {
        return bzPopMin.response();
    }
}
