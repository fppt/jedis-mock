package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.SliceParser;
import com.github.fppt.jedismock.storage.BlockingManager;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.Arrays;
import java.util.Collections;
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
    private final boolean isInScript;
    private final OperationExecutorState state;
    private final BlockingManager blockingManager;

    BRPopLPush(OperationExecutorState state, List<Slice> params) {
        super(state, params);
        this.lock = state.lock();
        this.isInTransaction = state.isTransactionModeOn();
        //See AbstractBPop: a blocking command must not block inside a Lua script.
        this.isInScript = state.scriptingManager().isRunning();
        this.state = state;
        this.blockingManager = state.blockingManager();
    }

    protected void doOptionalWork() {
        Slice source = params().get(0);
        long timeoutNanos = toNanoTimeout(params().get(2).toString());

        if (timeoutNanos < 0) {
            throw new IllegalArgumentException("ERR timeout is negative");
        }

        count = getCount(source);
        if (isInTransaction || isInScript || count != 0) {
            //Inside MULTI or a Lua script we never block; otherwise the element
            //is already there.
            return;
        }

        long waitEnd = System.nanoTime() + timeoutNanos;
        long waitTimeNanos;
        //Register as a FIFO waiter so that, among several clients blocked on the
        //same key, the oldest one is served first (matching real Redis). Without
        //this, notifyAll() lets an arbitrary waiter steal the element, which can
        //leave an older waiter blocked forever.
        List<Slice> waitKeys = Collections.singletonList(source);
        boolean acquired = false;
        try (BlockingManager.Ticket ticket = blockingManager.register(waitKeys)) {
            while ((connected = state.isClientConnected()) &&
                    (waitTimeNanos = timeoutNanos == 0 ? Long.MAX_VALUE : waitEnd - System.nanoTime()) >= 0) {
                if (getCount(source) != 0 && ticket.isFirst(source)) {
                    acquired = true;
                    break;
                }
                long remainingMillis = waitTimeNanos / 1_000_000;
                long waitMillis = Math.min(remainingMillis, POLL_MILLIS);
                int waitNano = waitMillis == remainingMillis ? (int) (waitTimeNanos % 1_000_000) : 0;
                lock.wait(waitMillis, waitNano);
            }
        } catch (InterruptedException e) {
            //wait interrupted prematurely
            Thread.currentThread().interrupt();
        } finally {
            //Hand the turn to the next-in-line waiter, which re-evaluates whether
            //it is now the oldest one able to claim the element. The ticket has
            //already been unregistered by try-with-resources.
            lock.notifyAll();
        }
        //Only claim the element if we actually won our turn; otherwise behave as
        //if nothing is available (timed out / disconnected / yielded to an older
        //waiter) so response() doesn't pop data meant for another client.
        count = acquired ? getCount(source) : 0;
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
