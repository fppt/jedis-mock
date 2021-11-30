package com.github.fppt.jedismock.operations.pubsub;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.RedisClient;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.Set;

@RedisCommand("publish")
class Publish extends AbstractRedisOperation {

    Publish(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response(){
        Slice channel = params().get(0);
        Slice message = params().get(1);

        Set<RedisClient> subscibers = base().getSubscribers(channel);

        subscibers.forEach(subscriber -> {
            Slice response = null;
            response = Response.publishedMessage(channel, message);
            subscriber.sendResponse(response, "contacting subscriber");
        });

        return Response.integer(subscibers.size());
    }
}
