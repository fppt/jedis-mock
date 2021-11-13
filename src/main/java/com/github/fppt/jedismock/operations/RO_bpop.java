package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.server.SliceParser;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToLong;

abstract class RO_bpop extends AbstractRedisOperation {

    private final Object lock;

    RO_bpop(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        this.lock = state.lock();
    }

    abstract AbstractRedisOperation popper(List<Slice> params);

    Slice response() {
        int size = params().size();
        if (size < 2) {
            throw new IndexOutOfBoundsException("require at least 2 params");
        }
        List<Slice> keys = params().subList(0, size - 1);
        long timeout = convertToLong(params().get(size - 1).toString());
        Slice source = getKey(keys);
        long waitEnd = System.nanoTime() + timeout * 1_000_000_000L;
        long waitTime;
        try {
            while (source == null && (waitTime = (waitEnd - System.nanoTime()) / 1_000_000L) > 0) {
                lock.wait(waitTime);
                source = getKey(keys);
            }
        } catch (InterruptedException e) {
            //wait interrupted prematurely
            Thread.currentThread().interrupt();
            return Response.NULL;
        }
        if (source != null) {
            Slice result = popper(Collections.singletonList(source)).execute();
            return Response.array(Arrays.asList(Response.bulkString(source), result));
        } else {
            System.out.println("Source is still null");
            return Response.NULL;
        }
    }

    private Slice getKey(List<Slice> list) {
        for (Slice key : list) {
            Slice result = new RO_llen(base(), Collections.singletonList(key)).execute();
            int length = SliceParser.consumeInteger(result.data());
            if (length > 0) {
                return key;
            }
        }
        return null;
    }
}
