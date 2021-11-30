package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.server.SliceParser;

import java.util.Arrays;
import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToLong;

@RedisCommand("brpoplpush")
class BRPopLPush extends RPopLPush {
    private long count = 0L;
    private final Object lock;

    BRPopLPush(OperationExecutorState state, List<Slice> params) {
        super(state, params);
        this.lock = state.lock();
    }

    protected void doOptionalWork(){
        Slice source = params().get(0);
        long timeout = convertToLong(params().get(2).toString());
        long waitEnd = System.nanoTime() + timeout * 1_000_000_000L;
        long waitTime;
        count = getCount(source);
        try {
            while (count == 0L && (waitTime = (waitEnd - System.nanoTime()) / 1_000_000L) > 0) {
                lock.wait(waitTime);
                count = getCount(source);
            }
        } catch (InterruptedException e) {
            //wait interrupted prematurely
            Thread.currentThread().interrupt();
            return;
        }
    }

    protected Slice response() {
        if(count != 0){
            return super.response();
        } else {
            return Response.NULL;
        }
    }

    private long getCount(Slice source){
        Slice index = Slice.create("0");
        List<Slice> commands = Arrays.asList(source, index, index);
        Slice result = new LRange(base(), commands).execute();
        return SliceParser.consumeCount(result.data());
    }
}
