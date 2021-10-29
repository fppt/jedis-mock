package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.server.Slice;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.github.fppt.jedismock.Utils.serializeObject;

@TxOperation("sadd")
class RO_sadd extends AbstractRedisOperation {
    RO_sadd(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    Slice response() {
        Slice key = params().get(0);
        Set<Slice> set = getDataFromBase(key, new HashSet<>());

        int count = 0;
        for (int i = 1; i < params().size(); i++) {
            if (set.add(params().get(i))){
                count++;
            }
        }
        try {
            base().putValue(key, serializeObject(set));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        return Response.integer(count);
    }
}
