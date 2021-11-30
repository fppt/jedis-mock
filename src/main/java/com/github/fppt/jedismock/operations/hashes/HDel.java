package com.github.fppt.jedismock.operations.hashes;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("hdel")
class HDel extends AbstractRedisOperation {
    HDel(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response(){
        Slice key1 = params().get(0);
        Slice key2 = params().get(1);

        Slice oldValue = base().getSlice(key1, key2);

        base().deleteValue(key1, key2);

        if(oldValue == null){
            return Response.integer(0);
        } else {
            return Response.integer(1);
        }
    }
}
