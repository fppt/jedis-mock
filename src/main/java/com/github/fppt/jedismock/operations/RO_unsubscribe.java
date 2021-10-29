package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RO_unsubscribe extends AbstractRedisOperation {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RO_unsubscribe.class);
    private OperationExecutorState state;

    public RO_unsubscribe(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        this.state = state;
    }

    Slice response() {
        List<Slice> channelsToUbsubscribeFrom;
        if(params().isEmpty()){
            LOG.debug("No channels specified therefore unsubscribing from all channels");
            channelsToUbsubscribeFrom = base().getSubscriptions(state.owner());
        } else {
            channelsToUbsubscribeFrom = params();
        }

        for (Slice channel : channelsToUbsubscribeFrom) {
            LOG.debug("Unsubscribing from channel [" + channel + "]");
            if(base().removeSubscriber(channel, state.owner())) {
                int numSubscriptions = base().getSubscriptions(state.owner()).size();
                Slice response = Response.unsubscribe(channel, numSubscriptions);
                state.owner().sendResponse(Response.clientResponse("unsubscribe", response), "unsubscribe");
            }
        }

        //Skip is sent because we have already responded
        return Response.SKIP;
    }
}
