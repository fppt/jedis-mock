package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.server.SliceParser;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.github.fppt.jedismock.server.Response.NULL;

class RO_rpoplpush extends AbstractRedisOperation {
    RO_rpoplpush(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        Slice source = params().get(0);
        Slice target = params().get(1);

        //Pop last one
        Slice result = new RO_rpop(base(), Collections.singletonList(source)).execute();
        if(result.equals(NULL)) return NULL;

        Slice valueToPush = SliceParser.consumeParameter(result.data());

        //Push it into the other list
        new RO_lpush(base(), Arrays.asList(target, valueToPush)).execute();

        return result;
    }
}
