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

@RedisCommand("sdiff")
class SDiff extends AbstractRedisOperation {
    SDiff(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int minArgs() {
        return 1;
    }

    final Set<Slice> getDifference() {
        Slice key = params().get(0);
        RMSet setObj = getSetFromBaseOrCreateEmpty(key);
        Set<Slice> result = new HashSet<>(setObj.getStoredData());

        for(int i = 1; i < params().size(); i++){
            RMSet secondSetObj = getSetFromBaseOrCreateEmpty(params().get(i));
            Set<Slice> secondSet = secondSetObj.getStoredData();
            result.removeAll(secondSet);
        }

        return result;
    }

    @Override
    protected Slice response() {
        return Response.array(getDifference().stream().map(Response::bulkString).collect(toList()));
    }
}
