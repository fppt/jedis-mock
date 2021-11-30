package com.github.fppt.jedismock.operations.sets;

import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.Set;

@RedisCommand("sismember")
public class SIsMember extends AbstractRedisOperation {

    SIsMember(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        Slice member = params().get(1);
        RMSet setDBObj = getSetFromBaseOrCreateEmpty(key);
        Set<Slice> set = setDBObj.getStoredData();
        if (set == null || set.isEmpty()) return Response.integer(0);
        return Response.integer(set.contains(member) ? 1 : 0);
    }
}
