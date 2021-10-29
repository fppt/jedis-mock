package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;

@MetaOperation("subscribe")
public class RO_subscribe extends AbstractRedisOperation {
    private OperationExecutorState state;

    public RO_subscribe(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        this.state = state;
    }

    Slice response() {
        params().forEach(channel -> base().addSubscriber(channel, state.owner()));
        List<Slice> numSubscriptions = base().getSubscriptions(state.owner());

        return Response.subscribedToChannel(numSubscriptions);
    }
}
