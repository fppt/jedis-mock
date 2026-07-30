package com.github.fppt.jedismock.operations.strings;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.KeyspaceEvent;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("msetnx")
public class MSetNX extends AbstractRedisOperation {
    public MSetNX(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        RedisBase base = base();
        for (int i = 0; i < params().size(); i += 2) {
            if (base.exists(params().get(i))) {
                return Response.integer(0);
            }
        }
        for (int i = 0; i < params().size(); i += 2) {
            base.putValue(params().get(i), params().get(i + 1).extract());
            base.notifyKeyspaceEvent(KeyspaceEvent.SET, params().get(i));
        }
        return Response.integer(1);
    }
}
