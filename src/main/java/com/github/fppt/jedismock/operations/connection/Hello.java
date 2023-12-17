package com.github.fppt.jedismock.operations.connection;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.Arrays;
import java.util.List;

@RedisCommand(value = "hello", transactional = false)
public class Hello implements RedisOperation {
    private final Slice protover;

    Hello(List<Slice> params) {
        super();
        protover = params.isEmpty() ? null : params.get(0);
    }

    @Override
    public Slice execute() {
        if (protover != null && "3".equals(protover.toString())) {
            return Response.error("NOPROTO Resp3 not supported by JedisMock");
        } else {
            return Response.array(
                    Arrays.asList(
                            Response.bulkString(Slice.create("proto")),
                            Response.bulkString(Response.integer(2))
                    )
            );
        }
    }
}
