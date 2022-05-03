package com.github.fppt.jedismock.operations.strings;

import com.github.fppt.jedismock.datastructures.RMString;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("strlen")
class StrLen extends AbstractRedisOperation {
    StrLen(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        RMString value = base().getRMString(params().get(0));
        if (value == null) {
            return Response.integer(0);
        }
        return Response.integer(value.size());
    }
}
