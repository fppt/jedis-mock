package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

@RedisCommand("spop")
class RO_spop extends AbstractRedisOperation {
    RO_spop(RedisBase base, List<Slice> params ) {
        super(base, params);
    }

    Slice popper(Set<Slice> collection) {
        Iterator<Slice> it = collection.iterator();
        Slice v = it.next();
        it.remove();
        return v;
    }

    Slice response() {
        Slice key = params().get(0);
        final RMSet setDBObj = getSetFromBaseOrCreateEmpty(key);
        Set<Slice> data = setDBObj.getStoredData();
        if(data == null || data.isEmpty()) return Response.NULL;
        Slice v = popper(data);
        return Response.bulkString(v);
    }
}
