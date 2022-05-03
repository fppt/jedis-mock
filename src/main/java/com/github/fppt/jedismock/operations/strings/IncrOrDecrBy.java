package com.github.fppt.jedismock.operations.strings;

import com.github.fppt.jedismock.datastructures.RMString;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToLong;

abstract class IncrOrDecrBy extends AbstractRedisOperation {
    IncrOrDecrBy(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    abstract long incrementOrDecrementValue(List<Slice> params);

    protected Slice response() {
        Slice key = params().get(0);
        long d = incrementOrDecrementValue(params());
        RMString v = base().getRMString(key);

        if (v == null) {
            base().putValue(key, RMString.create(String.valueOf(d)));
            return Response.integer(d);
        }

        long r = convertToLong(v.getStoredData()) + d;
        base().putValueWithoutClearingTtl(key, RMString.create(String.valueOf(r)));
        return Response.integer(r);
    }
}
