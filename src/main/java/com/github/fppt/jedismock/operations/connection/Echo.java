package com.github.fppt.jedismock.operations.connection;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("echo")
class Echo extends AbstractRedisOperation {
    Echo(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        if (params().size() != 1) {
            return Response.error("ERR wrong number of arguments for 'echo' command");
        }
        return Response.bulkString(params().get(0));
    }
}
