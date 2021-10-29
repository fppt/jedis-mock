package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMDataStructure;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

class RO_rename extends AbstractRedisOperation {

    RO_rename(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    private boolean rename(Slice key, Slice newKey) {
        RMDataStructure value = base().getValue(key);
        final Long ttl = base().getTTL(key);
        if (ttl == null || value == null) {
            return false;
        }
        base().deleteValue(newKey);
        base().putValue(newKey, value, ttl);
        base().deleteValue(key);
        return true;
    }

    @Override
    Slice response() {
        final Slice key = params().get(0);
        final Slice newKey = params().get(1);
        if (!rename(key, newKey)) {
            return Response.error("ERR no such key");
        }
        return Response.OK;
    }
}
