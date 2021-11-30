package com.github.fppt.jedismock.operations.strings;

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
        Slice value = params().get(1);
        Slice s = base().getSlice(key);
        if (s == null) {
            base().putSlice(key, value);
            return Response.integer(value.length());
        }
        byte[] b = new byte[s.length() + value.length()];
        for (int i = 0; i < s.length(); i++) {
            b[i] = s.data()[i];
        }
        for (int i = s.length(); i < s.length() + value.length(); i++) {
            b[i] = value.data()[i - s.length()];
        }
        base().putSlice(key, Slice.create(b));
        return Response.integer(b.length);
    }
}
