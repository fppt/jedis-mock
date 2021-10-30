package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.Map;

@RedisCommand("hlen")
public class RO_hlen extends AbstractRedisOperation {
    public RO_hlen(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        Slice key = params().get(0);
        Map<Slice, Slice> map = base().getFieldsAndValues(key);
        return Response.integer(map.size());
    }
}
