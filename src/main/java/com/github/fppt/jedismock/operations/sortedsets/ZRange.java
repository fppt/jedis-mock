package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.fppt.jedismock.Utils.convertToInteger;

@RedisCommand("zrange")
class ZRange extends AbstractByScoreOperation {

    private static final String WITH_SCORES = "WITHSCORES";
    private static final String IS_REV = "REV";
    private static final String IS_BYSCORE = "BYSCORE";
    private static final String IS_BYLEX = "BYLEX";

    private boolean withScores = false;
    private boolean isRev = false;
    private boolean isByScore = false;
    private boolean isByLex = false;
    private int start = 0;
    private int end = 0;

    ZRange(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        final RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);

        parseArgs();

        if (isByScore && !isRev) {
            ZRangeByScore zRangeByScore = new ZRangeByScore(base(), params());
            return zRangeByScore.response();
        }
        if (isByScore) {
            ZRevRangeByScore zRevRangeByScore = new ZRevRangeByScore(base(), params());
            return zRevRangeByScore.response();
        }
        if (isByLex && !isRev) {
            ZRangeByLex zRangeByLex = new ZRangeByLex(base(), params());
            return zRangeByLex.response();
        }
        if (isByLex) {
            ZRevRangeByLex zRevRangeByLex = new ZRevRangeByLex(base(), params());
            return zRevRangeByLex.response();
        }

        calculateIndexes(mapDBObj);

        boolean finalWithScores = withScores;

        final List<Slice> values = mapDBObj.entries(isRev).stream()
                .skip(start)
                .limit(end - start + 1)
                .flatMap(e -> finalWithScores
                        ? Stream.of(e.getValue(), Slice.create(Double.toString(e.getScore())))
                        : Stream.of(e.getValue()))
                .map(Response::bulkString)
                .collect(Collectors.toList());

        return Response.array(values);
    }

    private void calculateIndexes(RMZSet map) {
        start = convertToInteger(params().get(1).toString());
        end = convertToInteger(params().get(2).toString());

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
    }

    private void parseArgs() {
        for (Slice param : params()) {
            if (WITH_SCORES.equalsIgnoreCase(param.toString())) {
                withScores = true;
            }
            if (IS_REV.equalsIgnoreCase(param.toString())) {
                isRev = true;
            }
            if (IS_BYSCORE.equalsIgnoreCase(param.toString())) {
                isByScore = true;
            }
            if (IS_BYLEX.equalsIgnoreCase(param.toString())) {
                isByLex = true;
            }
        }
    }
}
