package com.github.fppt.jedismock.operations.hashes;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("hstrlen")
public class HStrlen extends AbstractRedisOperation {
    public HStrlen(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        Slice field = params().get(1);
        Slice result = base().getFieldsAndValues(key).get(field);
        return Response.integer(result == null ? 0 : result.toString().length());
    }
}
