package com.github.fppt.jedismock.operations.connection;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;

@RedisCommand(value = "client", transactional = false)
public class Client implements RedisOperation {

    @Override
    public Slice execute() {
        return Response.clientResponse("client", Response.OK);
    }
}
