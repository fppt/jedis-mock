package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@TxOperation("zcard")
class RO_zcard extends AbstractRedisOperation {

    RO_zcard(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    Slice response() {
        Slice key = params().get(0);
        Map<Slice, Double> map = getDataFromBase(key, new LinkedHashMap<>());
        if (map == null || map.isEmpty()) return Response.integer(0);
        return Response.integer(map.size());
    }
}
