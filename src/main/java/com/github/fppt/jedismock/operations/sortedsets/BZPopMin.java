package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RedisCommand("bzpopmin")
public class BZPopMin extends AbstractByScoreOperation {
    private boolean isRev = false;

    BZPopMin(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    private Slice pop() {
        for (int i = 0; i < params().size() - 1; i++) {
            final Slice key = params().get(i);
            if (base().exists(key)) {
                final RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);
                List<Slice> result = new ArrayList<>();
                result.add(Response.bulkString(key));
                ZSetEntry entry = mapDBObj.entries(isRev).first();
                result.addAll(Stream.of(entry.getValue(),
                                Slice.create(Integer.toString((int) Math.round(entry.getScore()))))
                        .map(Response::bulkString)
                        .collect(Collectors.toList()));
                mapDBObj.remove(entry.getValue());
                if (mapDBObj.isEmpty()) {
                    base().deleteValue(key);
                }
                return Response.array(result);
            }
        }
        return Response.NULL_ARRAY;
    }

    @Override
    protected Slice response() {
        Slice result = Response.NULL_ARRAY;
        final long timeout = Math.round(1000 * Double.parseDouble(params().get(params().size() - 1).toString()));
        final boolean[] flag = {true};

        if (timeout > 0) {
            Callable<Slice> callable = () -> {
                Slice res = Response.NULL_ARRAY;
                while (flag[0] && res == Response.NULL_ARRAY) {
                    res = pop();
                }
                return res;
            };

            Runnable runnable = () -> {
                try {
                    Thread.sleep(timeout);
                    flag[0] = false;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            };
            FutureTask<Slice> futureTask = new FutureTask<>(callable);
            Thread findThread = new Thread(futureTask);
            Thread timeThread = new Thread(runnable);
            findThread.start();
            timeThread.start();
            try {
                findThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } else {
            while (result == Response.NULL_ARRAY) {
                result = pop();
            }
        }



        return result;
    }

    public void setRev(boolean rev) {
        isRev = rev;
    }

}
