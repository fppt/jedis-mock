package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.List;

class RO_lpushx extends RO_lpush {
    RO_lpushx(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response(){
        Slice key = params().get(0);
        Slice data = base().getSlice(key);

        if(data != null){
            return super.response();
        }

        return Response.integer(0);
    }
}
