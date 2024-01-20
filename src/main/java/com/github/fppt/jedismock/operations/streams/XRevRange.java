package com.github.fppt.jedismock.operations.streams;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("xrevrange")
public class XRevRange extends Ranges {
    public XRevRange(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int minArgs() {
        return 3;
    }

    @Override
    protected Slice response() {
        multiplier = -1;
        return range();
    }
}
