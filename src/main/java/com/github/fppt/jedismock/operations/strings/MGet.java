package com.github.fppt.jedismock.operations.strings;

import com.github.fppt.jedismock.datastructures.RMString;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import static java.util.stream.Collectors.toList;

import java.util.List;

@RedisCommand("mget")
class MGet extends AbstractRedisOperation {
    MGet(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        RedisBase base = base();
        return Response.array(params().stream()
                .map(base::getValue)
                .map(s -> s instanceof RMString ? s.getAsSlice() : null)
                .map(Response::bulkString)
                .collect(toList()));
    }
}
