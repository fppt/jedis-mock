package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("ttl")
class RO_ttl extends AbstractRedisOperation {
    RO_ttl(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice finalReturn(Long pttl){
        return Response.integer((pttl + 999) / 1000);
    }

    Slice response() {
        Long pttl = base().getTTL(params().get(0));
        if (pttl == null) {
            return Response.integer(-2L);
        }
        if (pttl == -1) {
            return Response.integer(-1L);
        }
        return finalReturn(pttl);
    }
}
