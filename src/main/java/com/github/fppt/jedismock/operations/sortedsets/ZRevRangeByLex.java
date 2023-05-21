package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.Collections;
import java.util.List;

@RedisCommand("zrevrangebylex")
class ZRevRangeByLex extends ZRangeByLex {

    ZRevRangeByLex(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected List<Slice> doProcess(RMZSet map, String start, String end, double score) {
        return process(map, start, end, score, true);
    }

    @Override
    protected String min() {
        return params().get(2).toString();
    }

    @Override
    protected String max() {
        return params().get(1).toString();
    }
}
