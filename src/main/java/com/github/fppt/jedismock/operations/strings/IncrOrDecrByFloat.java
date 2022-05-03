package com.github.fppt.jedismock.operations.strings;

import com.github.fppt.jedismock.datastructures.RMString;
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

        RMString foundValue = base().getRMString(key);
        if (foundValue != null) {
            numericValue = numericValue.add(new BigDecimal(foundValue.getStoredData()));
        }

        String data = String.valueOf(BigDecimal.valueOf(numericValue.intValue()).compareTo(numericValue) == 0
                ? numericValue.intValue() : numericValue);

        RMString res = RMString.create(data);
        base().putValue(key, res);

        return Response.bulkString(res.getAsSlice());
    }
}
