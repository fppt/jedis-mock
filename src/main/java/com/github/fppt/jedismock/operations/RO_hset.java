package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("hset")
class RO_hset extends AbstractRedisOperation {
    RO_hset(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice hsetValue(Slice key1, Slice key2, Slice value){
        Slice foundValue = base().getSlice(key1, key2);
        base().putSlice(key1, key2, value, -1L);
        return foundValue;
    }

    @Override
    Slice response() {
        Slice hash = params().get(0);
        int count = 0;

        for(int i = 1; i < params().size(); i = i + 2){
            Slice field = params().get(i);
            Slice value = params().get(i+1);
            Slice oldValue = hsetValue(hash, field, value);

            if(oldValue == null) {
                count++;
            }
        }

        return Response.integer(count);
    }
}
