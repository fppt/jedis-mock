package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMHMap;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.fppt.jedismock.Utils.convertToLong;

@RedisCommand("zrangebyscore")
public class ZRangeByScore extends AbstractByScoreOperation {
    public ZRangeByScore(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
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
        boolean isRev = false;
        for (int i = 3; i < params().size(); i++) {
            String param = params().get(i).toString();
            if ("withscores".equalsIgnoreCase(param)) {
                withScores = true;
            }
            if ("isRev".equalsIgnoreCase(param)) {
                isRev = true;
            } else if ("limit".equalsIgnoreCase(param)) {
                long offset = convertToLong(params().get(++i).toString());
                long count = convertToLong(params().get(++i).toString());
                entryStream = entryStream.skip(offset).limit(count);
            }
        }
        entryStream = entryStream.sorted(isRev ? ZRange.zRangeComparator.reversed() : ZRange.zRangeComparator);

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
