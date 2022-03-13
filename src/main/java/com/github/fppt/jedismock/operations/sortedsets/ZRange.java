package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMHMap;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.fppt.jedismock.Utils.convertToInteger;

@RedisCommand("zrange")
class ZRange extends AbstractRedisOperation {

    private static final String WITH_SCORES = "WITHSCORES";
    private static final String IS_REV = "REV";

    private static final Comparator<Map.Entry<Slice, Double>> zRangeComparator = Comparator
            .comparingDouble((ToDoubleFunction<Map.Entry<Slice, Double>>) Map.Entry::getValue)
            .thenComparing(Map.Entry::getKey);

    ZRange(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        final RMHMap mapDBObj = getHMapFromBaseOrCreateEmpty(key);
        final Map<Slice, Double> map = mapDBObj.getStoredData();

        int start = convertToInteger(params().get(1).toString());
        int end = convertToInteger(params().get(2).toString());

        if (start < 0) {
            start = map.size() + start;
            if (start < 0) {
                start = 0;
            }
        }

        if (end < 0) {
            end = map.size() + end;
            if (end < 0) {
                end = -1;
            }
        }

        if (end >= map.size()) {
            end = map.size() - 1;
        }

        boolean withScores = false;
        boolean isRev = false;

        for (Slice param : params()) {
            if (WITH_SCORES.equalsIgnoreCase(param.toString())){
                withScores = true;
            }
            if (IS_REV.equalsIgnoreCase(param.toString())){
                isRev = true;
            }
        }

        boolean finalWithScores = withScores;
        List<Slice> values = map.entrySet().stream()
                .sorted(isRev ? zRangeComparator.reversed() : zRangeComparator)
                .skip(start)
                .limit(end - start + 1)
                .flatMap(e -> finalWithScores
                        ? Stream.of(e.getKey(), Slice.create(e.getValue().toString()))
                        : Stream.of(e.getKey()))
                .map(Response::bulkString)
                .collect(Collectors.toList());

        return Response.array(values);
    }
}
