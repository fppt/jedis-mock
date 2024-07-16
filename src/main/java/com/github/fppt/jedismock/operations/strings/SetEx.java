package com.github.fppt.jedismock.operations.strings;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToLong;

@RedisCommand("setex")
class SetEx extends Set {
    SetEx(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    long timeoutToSet(List<Slice> params) {
        return Math.multiplyExact(convertToLong(new String(params.get(1).data())), 1000L);
    }

    protected Slice response() {
        final long timeout;
        try {
            timeout = timeoutToSet(params());
            Math.addExact(System.currentTimeMillis(), timeout);
        } catch (ArithmeticException e) {
            return Response.error(String.format("ERR invalid expire time in '%s' command", self().value()));
        }
        if (timeout <= 0){
            return Response.error(String.format("ERR invalid expire time in '%s' command", self().value()));
        }
        base().putValue(params().get(0), params().get(2).extract(), timeout);
        return Response.OK;
    }
}
