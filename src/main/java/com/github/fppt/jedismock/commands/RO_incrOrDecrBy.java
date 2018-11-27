package com.github.fppt.jedismock.commands;

import com.github.fppt.jedismock.RedisBase;
import com.github.fppt.jedismock.Response;
import com.github.fppt.jedismock.Slice;

import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToLong;

abstract class RO_incrOrDecrBy extends AbstractRedisOperation {
    RO_incrOrDecrBy(RedisBase base, List<Slice> params, Integer expectedParams) {
        super(base, params, expectedParams, null, null);
    }

    abstract long incrementOrDecrementValue(List<Slice> params);

    Slice response() {
        Slice key = params().get(0);
        long d = incrementOrDecrementValue(params());
        Slice v = base().getValue(key);
        if (v == null) {
            base().putValue(key, Slice.create(String.valueOf(d)));
            return Response.integer(d);
        }

        long r = convertToLong(new String(v.data())) + d;
        base().putValue(key, Slice.create(String.valueOf(r)));
        return Response.integer(r);
    }
}
