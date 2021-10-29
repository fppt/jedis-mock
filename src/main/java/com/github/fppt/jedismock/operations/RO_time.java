package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.Arrays;
import java.util.List;

@TxOperation("time")
public class RO_time extends AbstractRedisOperation {
    RO_time(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    Slice response() {
        //Java8 has wallclock with microseconds precision,
        //so this mock returns results truncated up to a millisecond
        long time = System.currentTimeMillis();
        long seconds = time / 1000L;
        long microseconds = (time % 1000L) * 1000L;
        return Response.array(Arrays.asList(
                Response.bulkString(Slice.create(Long.toString(seconds))),
                Response.bulkString(Slice.create(Long.toString(microseconds)))));
    }
}
