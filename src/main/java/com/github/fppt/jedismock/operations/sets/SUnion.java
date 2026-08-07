package com.github.fppt.jedismock.operations.sets;

import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toList;

import java.util.HashSet;

@RedisCommand("sunion")
class SUnion extends AbstractRedisOperation {
    SUnion(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int minArgs() {
        return 1;
    }

    final Set<Slice> getUnion() {
        Slice key = params().get(0);
        RMSet setObj = getSetFromBaseOrCreateEmpty(key);
        Set<Slice> resultSoFar = new HashSet<>(setObj.getStoredData());

        for(int i = 1; i < params().size(); i++){
            RMSet secondSetObj = getSetFromBaseOrCreateEmpty(params().get(i));
            Set<Slice> secondSet = secondSetObj.getStoredData();
            resultSoFar.addAll(secondSet);
        }

        return resultSoFar;
    }

    @Override
    protected Slice response() {
        return Response.array(getUnion().stream().map(Response::bulkString).collect(toList()));
    }
}
