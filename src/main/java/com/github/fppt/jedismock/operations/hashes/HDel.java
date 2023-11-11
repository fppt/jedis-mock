package com.github.fppt.jedismock.operations.hashes;

import com.github.fppt.jedismock.exception.WrongValueTypeException;
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
        Slice key = params().get(0);
        int count = 0;

        if (params().size() < 2) {
            throw new WrongValueTypeException("ERR wrong number of arguments for 'hdel' command");
        }

        for (int i = 1; i < params().size(); ++i) {
            Slice currKey = params().get(i);
            Slice oldValue = base().getSlice(key, currKey);
            base().deleteValue(key, currKey);

            if (oldValue != null) {
                ++count;
            }
        }

        return Response.integer(count);
    }
}
