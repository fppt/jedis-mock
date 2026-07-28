package com.github.fppt.jedismock.operations.pubsub;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.RedisClient;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;
import java.util.Map;
import java.util.Set;

@RedisCommand("publish")
class Publish extends AbstractRedisOperation {
    private final OperationExecutorState state;

    Publish(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        this.state = state;
    }

    protected Slice response(){
        Slice channel = params().get(0);
        Slice message = params().get(1);

        Set<RedisClient> subscribers = state.subscriptionRegistry().getSubscribers(channel);

        subscribers.forEach(subscriber -> {
            Slice response = Response.publishedMessage(channel, message);
            subscriber.sendResponse(response, "contacting subscriber");
        });

        Map<Slice, Set<RedisClient>> patternsPsubscribers = state.subscriptionRegistry().getPsubscribers(channel);
        int totalClientsPsubscribed = patternsPsubscribers.entrySet().stream().map(mapEntry -> {
            Slice pattern = mapEntry.getKey();
            Set<RedisClient> psubscribedClients = mapEntry.getValue();
            psubscribedClients.forEach(psubscriber->{
                Slice response = Response.publishedPMessage(pattern, channel, message);
                psubscriber.sendResponse(response, "contacting subscriber");
            });
            return psubscribedClients.size();
        }).reduce(0, Integer::sum);
        return Response.integer(subscribers.size() + totalClientsPsubscribed);
    }
}
