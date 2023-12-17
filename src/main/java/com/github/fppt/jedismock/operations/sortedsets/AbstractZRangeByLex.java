package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.datastructures.ZSetEntryBound;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

abstract class AbstractZRangeByLex extends AbstractZRange {

    static final String NEGATIVELY_INFINITE = "-";
    static final String POSITIVELY_INFINITE = "+";
    static final String INCLUSIVE_PREFIX = "[";
    static final String EXCLUSIVE_PREFIX = "(";


    AbstractZRangeByLex(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    public final ZSetEntryBound getStartBound(Slice startSlice) {
        double score = getScoreFromSlice(startSlice);
        String start = startSlice.toString();
        if (NEGATIVELY_INFINITE.equals(start)) {
            return new ZSetEntryBound(score, ZSetEntry.MIN_VALUE, true);
        } else if (POSITIVELY_INFINITE.equals(start)) {
            return new ZSetEntryBound(score, ZSetEntry.MAX_VALUE, false);
        } else {
            return new ZSetEntryBound(score, Slice.create(start.substring(1)), start.startsWith(INCLUSIVE_PREFIX));
        }
    }

    private double getScoreFromSlice(Slice value) {
        if (startsWithAnyPrefix(value.toString())) {
            String valueString = value.toString().substring(1);
            Slice valueSlice = Slice.create(valueString);
            if (mapDBObj.hasMember(valueSlice)) {
                return mapDBObj.getScore(valueSlice);
            } else {
                return mapDBObj.entries(false).first().getScore();
            }
        } else {
            return mapDBObj.entries(false).first().getScore();
        }
    }

    @Override
    public final ZSetEntryBound getEndBound(Slice endSlice) {
        return getStartBound(endSlice);
    }

    protected Slice buildErrorResponse(String param) {
        return Response.error("*ERR*not*string*: Valid " + param + " must start with '" + INCLUSIVE_PREFIX + "' or '"
                + EXCLUSIVE_PREFIX + "' or unbounded");
    }

    protected boolean invalidateStart(String start) {
        return !NEGATIVELY_INFINITE.equals(start) && !startsWithAnyPrefix(start);
    }

    protected boolean invalidateEnd(String end) {
        return !POSITIVELY_INFINITE.equals(end) && !startsWithAnyPrefix(end);
    }

    protected boolean startsWithAnyPrefix(String s) {
        return s.startsWith(INCLUSIVE_PREFIX) || s.startsWith(EXCLUSIVE_PREFIX);
    }

}
