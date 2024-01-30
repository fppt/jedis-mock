package com.github.fppt.jedismock.operations.transactions;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static com.github.fppt.jedismock.storage.OperationExecutorState.TransactionState.ERRORED;

@RedisCommand(value = "exec", transactional = false)
public class Exec implements RedisOperation {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Exec.class);
    private final OperationExecutorState state;

    public Exec(OperationExecutorState state) {
        this.state = state;
    }

    @Override
    public Slice execute() {
        if (!state.isTransactionModeOn()) {
            return Response.error("ERR EXEC without MULTI");
        }

        if (state.getTransactionState() == ERRORED) {
            state.transactionMode(false);
            state.tx().clear();
            return Response.error("EXECABORT Transaction discarded because of previous errors.");
        }

        try {
            state.checkWatchedKeysNotExpired();
            boolean validTransaction = state.isValid();
            state.unwatch();
            state.transactionMode(false);
            if (!validTransaction) {
                state.tx().clear();
                return Response.NULL;
            }
            List<Slice> results = state.tx().stream().
                    map(RedisOperation::execute).
                    collect(Collectors.toList());
            state.tx().clear();
            return Response.array(results);
        } catch (Throwable t) {
            LOG.error("ERROR during committing transaction", t);
            return Response.NULL;
        }
    }
}
