package com.github.fppt.jedismock.operations.pubsub;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

@RedisCommand(value = "punsubscribe", transactional = false)
public class PUnsubscribe extends AbstractRedisOperation {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Unsubscribe.class);
    private OperationExecutorState state;

    public PUnsubscribe(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        this.state = state;
    }

    protected Slice response() throws IOException {
        List<Slice> channelsToUbsubscribeFrom;
        if(params().isEmpty()){
            LOG.debug("No channels specified therefore unsubscribing from all channels");
            channelsToUbsubscribeFrom = base().getPSubscriptions(state.owner());
        } else {
            channelsToUbsubscribeFrom = params();
        }

        for (Slice channel : channelsToUbsubscribeFrom) {
            LOG.debug("PUnsubscribing from channel [" + channel + "]");
            if(base().removePSubscriber(channel, state.owner())) {
                int numSubscriptions = base().getPSubscriptions(state.owner()).size();
                Slice response = Response.punsubscribe(channel, numSubscriptions);
                state.owner().sendResponse(Response.clientResponse("punsubscribe", response), "punsubscribe");
            }
        }

        //Skip is sent because we have already responded
        return Response.SKIP;
    }
}
