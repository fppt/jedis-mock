package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

@MetaOperation("exec")
public class RO_exec implements RedisOperation {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RO_exec.class);
    private OperationExecutorState state;

    public RO_exec(OperationExecutorState state) {
        this.state = state;
    }

    @Override
    public Slice execute() {
        try {
            state.transactionMode(false);
            List<Slice> results = state.tx().stream().
                    map(RedisOperation::execute).
                    collect(Collectors.toList());
            state.tx().clear();
            return Response.array(results);
        } catch (Throwable t){
            LOG.error("ERROR during committing transaction", t);
            return Response.NULL;
        }
    }
}
