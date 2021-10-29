package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToLong;

@TxOperation("pexpireat")
class RO_pexpireat extends AbstractRedisOperation {
    RO_pexpireat(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        long deadline = convertToLong(new String(params().get(1).data()));
        return Response.integer(base().setDeadline(params().get(0), deadline));
    }
}
