package com.github.fppt.jedismock.operations.hashes;

import com.github.fppt.jedismock.datastructures.RMHash;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("hlen")
public class HLen extends AbstractRedisOperation {
    public HLen(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        Slice key = params().get(0);
        RMHash map = base().getHash(key);
        return Response.integer(map == null ? 0 : map.sizeIncludingExpired());
    }
}
