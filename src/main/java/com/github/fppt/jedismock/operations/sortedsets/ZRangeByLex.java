package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.datastructures.ZSetEntryBound;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@RedisCommand("zrangebylex")
class ZRangeByLex extends AbstractRedisOperation {

    static final String NEGATIVELY_INFINITE = "-";
    static final String POSITIVELY_INFINITE = "+";
    static final String INCLUSIVE_PREFIX = "[";
    static final String EXCLUSIVE_PREFIX = "(";

    ZRangeByLex(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);

        String start = min();
        if (!validateStart(start)) {
            return buildErrorResponse("start");
        }

        String end = max();
        if (!validateEnd(end)) {
            return buildErrorResponse("end");
        }

        if (mapDBObj.isEmpty()) {
            return Response.array(Collections.emptyList());
        } else {
            //We assume that all the elements have the same score
            double score = mapDBObj.entries(false).first().getScore();
            return Response.array(doProcess(mapDBObj, start, end, score));
        }
    }

    private Slice buildErrorResponse(String param) {
        return Response.error("Valid " + param + " must start with '" + INCLUSIVE_PREFIX + "' or '"
                    + EXCLUSIVE_PREFIX + "' or unbounded");        
    }
    
    protected boolean validateStart(String start) {
        return NEGATIVELY_INFINITE.equals(start) || startsWithAnyPrefix(start);
    }

    protected boolean validateEnd(String end) {
        return POSITIVELY_INFINITE.equals(end) || startsWithAnyPrefix(end);
    }
    
    protected boolean startsWithAnyPrefix(String s) {
        return s.startsWith(INCLUSIVE_PREFIX) || s.startsWith(EXCLUSIVE_PREFIX);
    }

    protected List<Slice> doProcess(RMZSet map, String start, String end, double score) {
        return map.subset(buildStartEntryBound(score, start),
                buildEndEntryBound(score, end))
                .stream()
                .map(ZSetEntry::getValue)
                .map(Response::bulkString)
                .collect(Collectors.toList());
    }

    protected ZSetEntryBound buildStartEntryBound(double score, String start) {
        if (NEGATIVELY_INFINITE.equals(start)) {
            return new ZSetEntryBound(score, ZSetEntry.MIN_VALUE, true);
        } else {
            return new ZSetEntryBound(score, Slice.create(start.substring(1)), start.startsWith(INCLUSIVE_PREFIX));
        }
    }

    protected ZSetEntryBound buildEndEntryBound(double score, String end)  {
        if (POSITIVELY_INFINITE.equals(end)) {
            return new ZSetEntryBound(score, ZSetEntry.MAX_VALUE, false);
        } else {
            return new ZSetEntryBound(score, Slice.create(end.substring(1)), end.startsWith(INCLUSIVE_PREFIX));
        }
    }

    protected String min(){
        return params().get(1).toString();
    }

    protected String max() {
        return params().get(2).toString();
    }
}
