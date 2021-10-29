package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.List;
import java.util.Set;

import static com.github.fppt.jedismock.Utils.serializeObject;

class RO_sadd extends AbstractRedisOperation {
    RO_sadd(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    Slice response() {
        Slice key = params().get(0);
        RMSet setDBObj = getSetFromBase(key);
        Set<Slice> set = setDBObj.getStoredData();

        int count = 0;
        for (int i = 1; i < params().size(); i++) {
            if (set.add(params().get(i))){
                count++;
            }
        }
        try {
            base().putSlice(key, serializeObject(set));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        return Response.integer(count);
    }
}
