package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@RedisCommand(value = "pubsub", transactional = false)
public class RO_pubsub extends AbstractRedisOperation {
    RO_pubsub(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    Slice response() throws IOException {
        Slice subcommand = params().get(0);
        if ("channels".equalsIgnoreCase(subcommand.toString())) {
            String pattern =
                    Utils.createRegexFromGlob(
                            params().size() > 1 ? params().get(1).toString() : "*");
            return Response.array(base().getChannels().stream().filter(
                    s -> s.toString().matches(pattern)
            ).map(Response::bulkString).collect(Collectors.toList()));
        } else {
            return Response.error(String.format("Unsupported operation: pubsub %s", subcommand.toString()));
        }
    }
}
