package com.github.fppt.jedismock.operations.keys;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToLong;

@RedisCommand("pexpire")
class PExpire extends AbstractRedisOperation {
    PExpire(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    long getValue(List<Slice> params){
        return convertToLong(new String(params.get(1).data()));
    }

    protected Slice response() {
        return Response.integer(base().setTTL(params().get(0), getValue(params())));
    }
}
