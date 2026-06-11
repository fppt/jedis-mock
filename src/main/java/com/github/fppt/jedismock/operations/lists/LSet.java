package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.Objects;

@RedisCommand("lset")
public class LSet extends AbstractRedisOperation {
    public LSet(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        int index = Utils.convertToInteger(params().get(1).toString());
        Slice element = params().get(2);

        if (!base().exists(key)) {
            throw new IllegalArgumentException("ERR no such key");
        }

        List<Slice> storedData = Objects.requireNonNull(base().getList(key)).getStoredData();

        if (index < 0) {
            index = storedData.size() + index;
        }

        if (index < 0 || index >= storedData.size()) {
            throw new IllegalArgumentException("ERR index out of range");
        }

        storedData.set(index, element);
        return Response.OK;
    }
}
