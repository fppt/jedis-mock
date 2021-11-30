package com.github.fppt.jedismock.operations.connection;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.Arrays;

@RedisCommand(value = "hello", transactional = false)
public class Hello implements RedisOperation {

    @Override
    public Slice execute() {
        return Response.array(
                Arrays.asList(
                        Response.bulkString(Slice.create("proto")),
                        Response.bulkString(Response.integer(2))
                )
        );
    }
}
