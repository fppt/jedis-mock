package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.Set;

class RO_scard extends AbstractRedisOperation {

    RO_scard(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        Slice key = params().get(0);
        RMSet setDBObj = getSetFromBase(key);
        Set<Slice> set = setDBObj.getStoredData();
        if(set == null || set.isEmpty()) return Response.integer(0);
        return Response.integer(set.size());
    }
}
