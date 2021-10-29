package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@TxOperation("hscan")
public class RO_hscan extends RO_scan {
    private Slice keySlice;

    RO_hscan(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    void doOptionalWork() {
        this.keySlice = params().get(0);
        this.cursorSlice = params().get(1);
    }

    @Override
    protected List<Slice> getMatchingValues(String regex, long cursor, long count) {
        Map<Slice, Slice> fieldAndValueMap = base().getFieldsAndValues(keySlice);
        this.size = fieldAndValueMap.size();
        return fieldAndValueMap.entrySet().stream().skip(cursor)
                .limit(count)
                .filter(e -> e.getKey().toString().matches(regex))
                .flatMap(e-> Stream.of(e.getKey(), e.getValue()))
                .map(Response::bulkString)
                .collect(Collectors.toList());
    }

}
