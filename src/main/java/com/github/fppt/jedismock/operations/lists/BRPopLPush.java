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
    /**
     * Upper bound on a single {@code wait()} so the loop wakes up periodically
     * to re-check whether the client is still connected, even with no notify
     * and an infinite ({@code timeout 0}) block.
     */
    private static final long POLL_MILLIS = 100L;

    private long count = 0L;
    //Records why the wait loop ended, so response() doesn't re-probe the
    //connection at delivery time (a transient probe failure would otherwise
    //drop a legitimate reply and hang the client).
    private boolean connected = true;
    private final Object lock;
    private final boolean isInTransaction;
    private final OperationExecutorState state;

    BRPopLPush(OperationExecutorState state, List<Slice> params) {
        super(state, params);
        this.lock = state.lock();
        this.isInTransaction = state.isTransactionModeOn();
        this.state = state;
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
                    (connected = state.isClientConnected()) &&
                    (waitTimeNanos = timeoutNanos == 0 ? Long.MAX_VALUE : waitEnd - System.nanoTime()) >= 0) {
                long remainingMillis = waitTimeNanos / 1_000_000;
                long waitMillis = Math.min(remainingMillis, POLL_MILLIS);
                int waitNano = waitMillis == remainingMillis ? (int) (waitTimeNanos % 1_000_000) : 0;
                lock.wait(waitMillis, waitNano);
                count = getCount(source);
            }
        } catch (InterruptedException e) {
            //wait interrupted prematurely
            Thread.currentThread().interrupt();
        }
    }

    protected Slice response() {
        if (!connected) {
            //Client disconnected while blocked: don't move anything, don't reply.
            return Response.SKIP;
        }
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
