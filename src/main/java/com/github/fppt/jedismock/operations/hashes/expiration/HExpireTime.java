package com.github.fppt.jedismock.operations.hashes.expiration;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("hexpiretime")
public class HExpireTime extends HPExpireTime {
    public HExpireTime(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice wrap(long deadline) {
        return Response.integer(deadline / 1000);
    }
}
