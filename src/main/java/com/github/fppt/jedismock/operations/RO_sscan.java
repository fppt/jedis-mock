package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class RO_sscan extends RO_scan {

    RO_sscan(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    private Slice keySlice;

    @Override
    void doOptionalWork() {
        this.keySlice = params().get(0);
        this.cursorSlice = params().get(1);
    }

    @Override
    protected List<Slice> getMatchingValues(String regex, long cursor, long count) {
        RMSet setDBObj = getSetFromBase(keySlice);
        Set<Slice> set = setDBObj.getStoredData();
        this.size = set.size();
        return set.stream().skip(cursor)
                .limit(count)
                .filter(x -> x.toString().matches(regex))
                .map(Response::bulkString)
                .collect(Collectors.toList());
    }
}
