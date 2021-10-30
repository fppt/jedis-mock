package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;
import com.google.common.collect.ImmutableList;

import java.util.List;

@RedisCommand("mget")
class RO_mget extends AbstractRedisOperation {
    RO_mget(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response(){
        ImmutableList.Builder<Slice> builder = new ImmutableList.Builder<Slice>();
        for (Slice key : params()) {
            builder.add(Response.bulkString(base().getSlice(key)));

        }
        return Response.array(builder.build());
    }
}
