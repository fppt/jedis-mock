package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

@RedisCommand("zcount")
public class ZCount extends AbstractByScoreOperation {
    public ZCount(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() throws IOException {
        final Slice key = params().get(0);
        final RMZSet mapDBObj = getHMapFromBaseOrCreateEmpty(key);
        final Map<Slice, Double> map = mapDBObj.getStoredData();

        if (map == null || map.isEmpty()) return
                Response.integer(0);

        final String start = params().get(1).toString();
        final String end = params().get(2).toString();
        Predicate<Double> filterPredicate = getFilterPredicate(start, end);

        long result = map.values().stream()
                .filter(filterPredicate)
                .count();
        return Response.integer(result);
    }
}
