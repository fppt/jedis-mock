package com.github.fppt.jedismock.operations.keys;

import com.github.fppt.jedismock.datastructures.RMDataStructure;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("rename")
class Rename extends AbstractRedisOperation {

    Rename(RedisBase base, List<Slice> params) {
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
    protected Slice response() {
        final Slice key = params().get(0);
        final Slice newKey = params().get(1);
        if (!rename(key, newKey)) {
            return Response.error("ERR no such key");
        }
        return Response.OK;
    }
}
