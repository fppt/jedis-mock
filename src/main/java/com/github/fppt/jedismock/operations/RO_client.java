package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;

@MetaOperation("client")
public class RO_client implements RedisOperation {

    @Override
    public Slice execute() {
        return Response.clientResponse("client", Response.OK);
    }
}
