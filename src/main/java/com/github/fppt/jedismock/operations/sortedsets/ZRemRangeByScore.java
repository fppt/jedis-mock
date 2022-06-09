package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;

@RedisCommand("zremrangebyscore")
public class ZRemRangeByScore extends AbstractByScoreOperation {

    ZRemRangeByScore(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        final Slice key = params().get(0);
        final RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);
        if (mapDBObj.isEmpty()) return Response.integer(0);
        final String start = params().get(1).toString();
        final String end = params().get(2).toString();
        final List<ZSetEntry> subset = new ArrayList<>(mapDBObj.subset(getStartBound(start), getEndBound(end)));
        for (ZSetEntry entry : subset) {
            mapDBObj.remove(entry.getValue());
        }
        base().putValue(key, mapDBObj);
        return Response.integer(subset.size());
    }
}
