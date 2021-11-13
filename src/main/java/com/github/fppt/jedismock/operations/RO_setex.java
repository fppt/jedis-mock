package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToLong;

@RedisCommand("setex")
class RO_setex extends RO_set {
    RO_setex(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    long timeoutToSet(List<Slice> params){
        return convertToLong(new String(params.get(1).data())) * 1000;
    }

    Slice response() {
        base().putSlice(params().get(0), params().get(2), timeoutToSet(params()));
        return Response.OK;
    }
}
