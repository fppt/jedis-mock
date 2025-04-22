package com.github.fppt.jedismock.operations.hashes.expiration;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("httl")
public class HTTL extends HPTTL {
    public HTTL(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice wrap(long pttl) {
        return Response.integer((pttl + 999) / 1000);
    }
}
