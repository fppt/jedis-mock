package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.datastructures.ZSetEntryBound;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.stream.Collectors;

import static com.github.fppt.jedismock.Utils.convertToInteger;
import static com.github.fppt.jedismock.operations.sortedsets.AbstractZRange.Options.REV;

abstract class AbstractZRangeByIndex extends AbstractZRange {

    AbstractZRangeByIndex(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    public final ZSetEntryBound getStartBound(Slice startSlice) {
        ZSetEntry entry = mapDBObj.entries(options.contains(REV)).stream()
                .skip(convertToInteger(startSlice.toString()))
                .limit(1)
                .collect(Collectors.toList())
                .get(0);
        return new ZSetEntryBound(entry, true);
    }

    @Override
    public final ZSetEntryBound getEndBound(Slice endSlice) {
        return getStartBound(endSlice);
    }

    protected boolean checkWrongIndex() {
        startIndex = convertToInteger(params().get(1).toString());
        endIndex = convertToInteger(params().get(2).toString());

        if (startIndex < 0) {
            startIndex = mapDBObj.size() + startIndex;
            if (startIndex < 0) {
                startIndex = 0;
            }
        }

        if (endIndex < 0) {
            endIndex = mapDBObj.size() + endIndex;
            if (endIndex < 0) {
                endIndex = -1;
            }
        }

        if (endIndex >= mapDBObj.size()) {
            endIndex = mapDBObj.size() - 1;
        }

        return startIndex > mapDBObj.size() || startIndex > endIndex;
    }
}
