package com.github.fppt.jedismock.operations.sets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.WrongValueTypeException;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.Arrays;
import java.util.List;

@RedisCommand("smove")
public class SMove extends AbstractRedisOperation {
    SMove(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice src = params().get(0);
        Slice dest = params().get(1);
        Slice member = params().get(2);

        // check destination type BEFORE deleting from src
        if (base().getValue(dest) != null && base().getSet(dest) == null) {
            throw new WrongValueTypeException("WRONGTYPE dest is not a set");
        }

        final int result = new SRem(base(), Arrays.asList(src, member)).remove();

        //The source is reported in full (including the del for the set it
        //emptied) before the destination's sadd
        if (result > 0) {
            SRem.publishRemoval(base(), src);
        }

        if (result > 0 && !getSetFromBaseOrCreateEmpty(dest).getStoredData().contains(member)) {
            new SAdd(base(), Arrays.asList(dest, member)).execute();
        }
        return Response.integer(result);
    }
}
