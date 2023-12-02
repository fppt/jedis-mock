package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.SliceParser;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.Arrays;
import java.util.List;

import static com.github.fppt.jedismock.Utils.toNanoTimeout;

@RedisCommand("brpoplpush")
class BRPopLPush extends RPopLPush {
    private long count = 0L;
    private final Object lock;
    private final boolean isInTransaction;

    BRPopLPush(OperationExecutorState state, List<Slice> params) {
        super(state, params);
        this.lock = state.lock();
        this.isInTransaction = state.isTransactionModeOn();
    }

    protected void doOptionalWork() {
        Slice source = params().get(0);
        long timeoutNanos = toNanoTimeout(params().get(2).toString());

        if (timeoutNanos < 0) {
            throw new IllegalArgumentException("ERR timeout is negative");
        }

        long waitEnd = System.nanoTime() + timeoutNanos;
        long waitTimeNanos;
        count = getCount(source);
        try {
            while (count == 0L &&
                    !isInTransaction &&
                    (waitTimeNanos = timeoutNanos == 0 ? 0 : waitEnd - System.nanoTime()) >= 0) {
                lock.wait(waitTimeNanos / 1_000_000, (int) waitTimeNanos % 1_000_000);
                count = getCount(source);
            }
        } catch (InterruptedException e) {
            //wait interrupted prematurely
            Thread.currentThread().interrupt();
        }
    }

    protected Slice response() {
        if (count != 0) {
            return super.response();
        } else {
            return Response.NULL;
        }
    }

    private long getCount(Slice source) {
        Slice index = Slice.create("0");
        List<Slice> commands = Arrays.asList(source, index, index);
        Slice result = new LRange(base(), commands).execute();
        return SliceParser.consumeCount(result.data());
    }
}
