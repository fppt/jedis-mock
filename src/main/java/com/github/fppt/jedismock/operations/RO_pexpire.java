package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToLong;

@TxOperation("pexpire")
class RO_pexpire extends AbstractRedisOperation {
    RO_pexpire(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    long getValue(List<Slice> params){
        return convertToLong(new String(params.get(1).data()));
    }

    Slice response() {
        return Response.integer(base().setTTL(params().get(0), getValue(params())));
    }
}
