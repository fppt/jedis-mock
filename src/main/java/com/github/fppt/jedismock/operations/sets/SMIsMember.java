package com.github.fppt.jedismock.operations.sets;

import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RedisCommand("smismember")
public class SMIsMember extends AbstractRedisOperation {
    SMIsMember(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int minArgs() {
        return 2;
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        Stream<Slice> paramStream = params().subList(1, params().size()).stream();
        RMSet setDBObj = getSetFromBaseOrCreateEmpty(key);
        Set<Slice> set = setDBObj.getStoredData();
        if (set == null || set.isEmpty()) {
            return Response.array(paramStream.map(el -> Response.integer(0)).collect(Collectors.toList()));
        }

        return Response.array(
                paramStream
                        .map(el -> set.contains(el) ? Response.integer(1) : Response.integer(0))
                        .collect(Collectors.toList())
        );
    }
}
