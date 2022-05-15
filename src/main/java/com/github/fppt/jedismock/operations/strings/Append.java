package com.github.fppt.jedismock.operations.strings;

import com.github.fppt.jedismock.datastructures.RMString;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("append")
class Append extends AbstractRedisOperation {
    Append(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        Slice key = params().get(0);
        String value = params().get(1).toString();
        RMString s = base().getRMString(key);

        if (s == null) {
            base().putValue(key, RMString.create(value));
            return Response.integer(value.length());
        }

        s.add(value);
        base().putValue(key, s);
        return Response.integer(s.size());
    }
}
