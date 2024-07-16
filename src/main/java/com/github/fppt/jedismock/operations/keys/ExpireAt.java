package com.github.fppt.jedismock.operations.keys;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToLong;

@RedisCommand("expireat")
class ExpireAt extends AbstractRedisOperation {
    ExpireAt(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int minArgs() {
        return 2;
    }

    protected Slice response() {
        long deadline = convertToLong(new String(params().get(1).data())) * 1000;
        return Response.integer(base().setDeadline(params().get(0), deadline));
    }
}
