package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("zcount")
public class ZCount extends AbstractByScoreOperation {
    public ZCount(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        final Slice key = params().get(0);
        final RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);

        if (mapDBObj.isEmpty()) return Response.integer(0);

        final String start = params().get(1).toString();
        final String end = params().get(2).toString();

        long result = mapDBObj.subset(getStartBound(start), getEndBound(end)).size();

        return Response.integer(result);
    }
}
