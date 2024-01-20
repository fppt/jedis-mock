package com.github.fppt.jedismock.operations.streams;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

/**
 * XRANGE key start end [COUNT count]<br>
 * Supported options: COUNT
 */
@RedisCommand("xrange")
public class XRange extends Ranges {
    XRange(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int minArgs() {
        return 3;
    }

    @Override
    protected Slice response() {
        multiplier = 1;
        return range();
    }
}
