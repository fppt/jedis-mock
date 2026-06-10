package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.WrongValueTypeException;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.SliceParser;
import com.github.fppt.jedismock.storage.BlockingManager;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.Collections;
import java.util.List;

import static com.github.fppt.jedismock.Utils.toNanoTimeout;

public abstract class AbstractBPop extends AbstractRedisOperation {
    /**
     * Upper bound on a single {@code wait()} so the loop wakes up periodically
     * to re-check whether the client is still connected, even with no notify
     * and an infinite ({@code timeout 0}) block.
     */
    private static final long POLL_MILLIS = 100L;

    protected long timeoutNanos;
    protected List<Slice> keys;
    private final Object lock;
    private final boolean isInTransaction;
    private final boolean isInScript;
    private final OperationExecutorState state;
    private final BlockingManager blockingManager;

    protected AbstractBPop(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        this.lock = state.lock();
        this.isInTransaction = state.isTransactionModeOn();
        //A blocking command invoked from a Lua script must not block: real Redis
        //runs it non-blockingly (scripts can't block). A script's redis.call is
        //dispatched while the script is running, so this is true exactly then.
        this.isInScript = state.scriptingManager().isRunning();
        this.state = state;
        this.blockingManager = state.blockingManager();
    }

    @Override
    protected int minArgs(){
        return 2;
    }

    @Override
    protected void doOptionalWork() {
        timeoutNanos = toNanoTimeout(params().get(params().size() - 1).toString());
        keys = params().subList(0, params().size() - 1);
    }

    protected abstract Slice popper(List<Slice> params);

    protected abstract AbstractRedisOperation getSize(RedisBase base, List<Slice> params);

    protected Slice response() {
        if (timeoutNanos < 0) {
            throw new IllegalArgumentException("ERR timeout is negative");
        }

        Slice source = getKey(keys, true);

        if (source != null || isInTransaction || isInScript) {
            //Element already available, or we must not block (inside MULTI, or
            //inside a Lua script — neither may block in real Redis).
            if (source == null) {
                return Response.NULL_ARRAY;
            }
            return popper(Collections.singletonList(source));
        }

        long waitEnd = System.nanoTime() + timeoutNanos;
        long waitTimeNanos;
        //Remember why the loop ended: we must not re-probe the connection at
        //delivery time. A transient liveness-probe failure right when data has
        //arrived would otherwise drop a legitimate reply and hang the client.
        boolean connected = true;
        //Register as a FIFO waiter on every key so that, among several clients
        //blocked on the same key, the oldest is served first (matching real
        //Redis). Without this, notifyAll() lets an arbitrary waiter steal the
        //element, which can leave an older waiter blocked forever.
        try (BlockingManager.Ticket ticket = blockingManager.register(keys)) {
            while ((connected = state.isClientConnected()) &&
                    (waitTimeNanos = timeoutNanos == 0 ? Long.MAX_VALUE : waitEnd - System.nanoTime()) >= 0) {
                Slice candidate = getKey(keys, false);
                if (candidate != null && ticket.isFirst(candidate)) {
                    source = candidate;
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
            return Response.NULL;
        } finally {
            //Hand the turn to the next-in-line waiter, which re-evaluates whether
            //it is now the oldest one able to claim an element. The ticket has
            //already been unregistered by try-with-resources.
            lock.notifyAll();
        }
        if (!connected) {
            //Client disconnected while blocked: don't consume anything, don't reply.
            return Response.SKIP;
        }
        if (source == null) {
            return Response.NULL_ARRAY;
        } else {
            return popper(Collections.singletonList(source));
        }
    }

    private Slice getKey(List<Slice> list, boolean checkForType) {
        for (Slice key : list) {
            if (!base().exists(key)) {
                continue;
            }
            Slice result;
            try {
                result = getSize(base(), Collections.singletonList(key)).execute();
            } catch (WrongValueTypeException e) {
                if (checkForType) {
                    throw e;
                }
                continue;
            }
            int length = SliceParser.consumeInteger(result.data());
            if (length > 0) {
                return key;
            }
        }
        return null;
    }
}
