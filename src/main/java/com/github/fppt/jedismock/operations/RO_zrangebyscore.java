package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMHMap;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.fppt.jedismock.Utils.convertToLong;

@RedisCommand("zrangebyscore")
public class RO_zrangebyscore extends AbstractByScoreOperation {
    public RO_zrangebyscore(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    Slice response() {
        final Slice key = params().get(0);
        final RMHMap mapDBObj = getHMapFromBaseOrCreateEmpty(key);
        final Map<Slice, Double> map = mapDBObj.getStoredData();

        if (map == null || map.isEmpty()) return
                Response.array(Collections.emptyList());

        final String start = params().get(1).toString();
        final String end = params().get(2).toString();
        Predicate<Double> filterPredicate = getFilterPredicate(start, end);

        Stream<Map.Entry<Slice, Double>> entryStream = map.entrySet().stream()
                .filter(e -> filterPredicate.test(e.getValue()));

        boolean withScores = false;
        for (int i = 3; i < params().size(); i++) {
            String param = params().get(i).toString();
            if ("withscores".equalsIgnoreCase(param)) {
                withScores = true;
            } else if ("limit".equalsIgnoreCase(param)) {
                long offset = convertToLong(params().get(++i).toString());
                long count = convertToLong(params().get(++i).toString());
                entryStream = entryStream.skip(offset).limit(count);
            }
        }

        Stream<Slice> result;
        if (withScores) {
            result = entryStream
                    .flatMap(e -> Stream.of(e.getKey(),
                            Slice.create(e.getValue().toString())));
        } else {
            result = entryStream
                    .map(Map.Entry::getKey);
        }
        return Response.array(result
                .map(Response::bulkString)
                .collect(Collectors.toList()));
    }
}
