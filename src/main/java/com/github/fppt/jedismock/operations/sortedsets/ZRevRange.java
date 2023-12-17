package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.exception.ArgumentException;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;

import static com.github.fppt.jedismock.operations.sortedsets.AbstractZRange.Options.BYLEX;
import static com.github.fppt.jedismock.operations.sortedsets.AbstractZRange.Options.BYSCORE;
import static com.github.fppt.jedismock.operations.sortedsets.AbstractZRange.Options.LIMIT;
import static com.github.fppt.jedismock.operations.sortedsets.AbstractZRange.Options.REV;

@RedisCommand("zrevrange")
class ZRevRange extends AbstractZRangeByIndex {

    ZRevRange(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        if (options.contains(BYSCORE) || options.contains(BYLEX) || options.contains(LIMIT)) {
            throw new ArgumentException("*syntax*");
        }
        key = params().get(0);
        mapDBObj = getZSetFromBaseOrCreateEmpty(key);
        options.add(REV);
        if (checkWrongIndex()) {
            return Response.array(new ArrayList<>());
        }

        NavigableSet<ZSetEntry> entries = getRange(
                getStartBound(Slice.create(String.valueOf(endIndex))),
                getEndBound(Slice.create(String.valueOf(startIndex))));

        return getSliceFromRange(entries);
    }
}
