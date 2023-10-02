package com.github.fppt.jedismock.operations.strings;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("getdel")
public class GetDel extends AbstractRedisOperation {
    public GetDel(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        Slice slice = base().getSlice(params().get(0));
        if (slice != null) {
            base().deleteValue(params().get(0));
        }
        return Response.bulkString(slice);
    }
}
