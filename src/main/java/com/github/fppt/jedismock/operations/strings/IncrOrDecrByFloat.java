package com.github.fppt.jedismock.operations.strings;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.math.BigDecimal;
import java.util.List;

abstract class IncrOrDecrByFloat extends AbstractRedisOperation {
    IncrOrDecrByFloat(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        Slice key = params().get(0);
        BigDecimal numericValue = new BigDecimal(params().get(1).toString());

        Slice foundValue = base().getSlice(key);
        if (foundValue != null) {
            numericValue = numericValue.add(new BigDecimal((new String(foundValue.data()))));
        }

        String data = String.valueOf(BigDecimal.valueOf(numericValue.intValue()).compareTo(numericValue) == 0
                ? numericValue.intValue() : numericValue);

        Slice res = Slice.create(data);
        base().putSlice(key, res);

        return Response.bulkString(res);
    }
}
