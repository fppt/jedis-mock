package com.github.fppt.jedismock.operations.keys;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.KeyspaceEvent;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("del")
class Del extends AbstractRedisOperation {
    Del(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        int count = 0;
        for (Slice key : params()) {
            //An already-expired key is reported as 'expired' by exists(), not deleted here
            if (base().exists(key)) {
                base().deleteValue(key);
                base().notifyKeyspaceEvent(KeyspaceEvent.DEL, key);
                count++;
            }
        }
        return Response.integer(count);
    }
}
