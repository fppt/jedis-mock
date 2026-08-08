package com.github.fppt.jedismock.operations.strings;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToLong;

@RedisCommand("decrby")
class DecrBy extends IncrOrDecrBy {
    DecrBy(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int maxArgs() {
        return 2;
    }

    long incrementOrDecrementValue(List<Slice> params) {
        long d = convertToLong(String.valueOf(params.get(1)));
        if (d == Long.MIN_VALUE) {
            throw new IllegalArgumentException("ERR decrement would overflow");
        } else {
            return -d;
        }
    }
}
