package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@TxOperation("del")
class RO_del extends AbstractRedisOperation {
    RO_del(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        int count = 0;
        for (Slice key : params()) {
            if (base().exists(key)) {
                base().deleteValue(key);
                count++;
            }
        }
        return Response.integer(count);
    }
}
