package com.github.fppt.jedismock.operations.hashes;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RedisCommand("hscan")
public class HScan extends com.github.fppt.jedismock.operations.keys.Scan {
    private Slice keySlice;

    HScan(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected void doOptionalWork() {
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
