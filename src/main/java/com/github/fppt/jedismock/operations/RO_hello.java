package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;

import java.util.Arrays;

public class RO_hello implements RedisOperation {

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
