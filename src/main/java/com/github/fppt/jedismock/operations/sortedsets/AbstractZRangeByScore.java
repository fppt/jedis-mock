package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.datastructures.ZSetEntryBound;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

abstract class AbstractZRangeByScore extends AbstractZRange {

    AbstractZRangeByScore(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    public final ZSetEntryBound getStartBound(Slice startSlice) {
        String start = startSlice.toString();
        if (LOWEST_POSSIBLE_SCORE.equalsIgnoreCase(start)) {
            return ZSetEntryBound.MINUS_INF;
        } else if (start.startsWith(EXCLUSIVE_PREFIX)) {
            return new ZSetEntryBound(toDouble(start.substring(1)), ZSetEntry.MAX_VALUE, false);
        } else {
            return new ZSetEntryBound(toDouble(start), ZSetEntry.MIN_VALUE, true);
        }
    }

    @Override
    public final ZSetEntryBound getEndBound(Slice endSlice) {
        String end = endSlice.toString();
        if (HIGHEST_POSSIBLE_SCORE.equalsIgnoreCase(end)) {
            return new ZSetEntryBound(Double.POSITIVE_INFINITY, ZSetEntry.MAX_VALUE, end.startsWith(EXCLUSIVE_PREFIX));
        } else if (end.startsWith(EXCLUSIVE_PREFIX)) {
            return new ZSetEntryBound(toDouble(end.substring(1)), ZSetEntry.MIN_VALUE, false);
        } else {
            return new ZSetEntryBound(toDouble(end), ZSetEntry.MAX_VALUE, true);
        }
    }

}
