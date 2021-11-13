package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMHMap;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@RedisCommand("zremrangebyscore")
public class RO_zremrangebyscore extends AbstractByScoreOperation {

    RO_zremrangebyscore(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    Slice response() {
        final Slice key = params().get(0);
        final RMHMap mapDBObj = getHMapFromBaseOrCreateEmpty(key);
        final Map<Slice, Double> map = mapDBObj.getStoredData();

        if (map == null || map.isEmpty()) return Response.integer(0);

        final String start = params().get(1).toString();
        final String end = params().get(2).toString();
        Predicate<Double> filterPredicate = getFilterPredicate(start, end);

        List<Double> values = map.values().stream()
                .filter(filterPredicate)
                .collect(Collectors.toList());

        final Map<Slice, Double> result = map.entrySet().stream()
                .filter(entry -> filterPredicate.negate().test(entry.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (u, v) -> {
                            //duplicate key
                            throw new IllegalStateException();
                        }, LinkedHashMap::new));

        try {
            base().putValue(key, new RMHMap(result));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }

        return Response.integer(values.size());
    }

}
