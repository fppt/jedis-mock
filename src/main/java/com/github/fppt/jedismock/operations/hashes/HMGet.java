package com.github.fppt.jedismock.operations.hashes;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;

@RedisCommand("hmget")
public class HMGet extends AbstractRedisOperation {
    public HMGet(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        ArrayList<Slice> result = new ArrayList<>();
        Slice hash = params().get(0);

        for(int i = 1; i < params().size(); i ++){
            Slice field = params().get(i);
            Slice value = base().getSlice(hash, field);

            if(value == null){
                result.add(Response.NULL);
            } else {
                result.add(Response.bulkString(value));
            }
        }

        return Response.array(result);
    }
}
