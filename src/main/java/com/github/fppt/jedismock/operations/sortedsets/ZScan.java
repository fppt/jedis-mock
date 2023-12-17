package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.keys.Scan;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.List;

@RedisCommand("zscan")
class ZScan extends Scan {
    ZScan(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    private Slice keySlice;

    @Override
    protected void doOptionalWork() {
        this.keySlice = params().get(0);
        this.cursorSlice = params().get(1);
    }

    @Override
    protected List<Slice> getMatchingValues(String regex, long cursor, long count) {
        RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(keySlice);
        return mapDBObj.entries(false).stream()
                .skip(cursor)
                .limit(count)
                .filter(e -> e.getValue().toString().matches(regex))
                .flatMap(e -> Stream.of(e.getValue(),
                        Slice.create(String.valueOf(e.getScore()))))
                .map(Response::bulkString)
                .collect(Collectors.toList());
    }

}
