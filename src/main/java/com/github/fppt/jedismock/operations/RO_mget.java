package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import static java.util.stream.Collectors.toList;

import java.util.Collections;
import java.util.List;

@RedisCommand("mget")
class RO_mget extends AbstractRedisOperation {
    RO_mget(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        return Response.array(Collections.unmodifiableList(params().stream().map(key -> Response.bulkString(base().getSlice(key))).collect(toList())));
    }
}
