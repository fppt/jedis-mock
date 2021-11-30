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

@RedisCommand("sinter")
class SInter extends AbstractRedisOperation {
    SInter(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        RMSet setObj = getSetFromBaseOrCreateEmpty(key);
        Set<Slice> resultSoFar = setObj.getStoredData();

        for(int i = 1; i < params().size(); i++){
            RMSet secondSetObj = getSetFromBaseOrCreateEmpty(params().get(i));
            Set<Slice> secondSet = secondSetObj.getStoredData();
            resultSoFar.retainAll(secondSet);
        }

        return Response.array(resultSoFar.stream().map(Response::bulkString).collect(toList()));
    }
}
