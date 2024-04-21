package com.github.fppt.jedismock.operations.hashes;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RedisCommand("hvals")
public class HVals extends AbstractRedisOperation {
    public HVals(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice hash = params().get(0);

        Map<Slice, Slice> fieldAndValueMap = base().getFieldsAndValues(hash);

        return Response.array(
                fieldAndValueMap.values().stream()
                        .map(Response::bulkString)
                        .collect(Collectors.toList())
        );
    }
}
