package com.github.fppt.jedismock.operations.keys;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.KeyspaceEvent;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("persist")
class Persist extends AbstractRedisOperation {
    Persist(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        Slice key = params().get(0);
        long result = base().setDeadline(key, -1);
        if (result == 1) {
            //Only an actually removed TTL is reported
            base().notifyKeyspaceEvent(KeyspaceEvent.PERSIST, key);
        }
        return Response.integer(result);
    }
}
