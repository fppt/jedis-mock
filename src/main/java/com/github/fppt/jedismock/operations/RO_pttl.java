package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.List;

@RedisCommand("pttl")
class RO_pttl extends RO_ttl {
    RO_pttl(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice finalReturn(Long pttl){
        return Response.integer(pttl);
    }
}
