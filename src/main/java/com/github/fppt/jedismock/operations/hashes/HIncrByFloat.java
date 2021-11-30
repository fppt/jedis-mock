package com.github.fppt.jedismock.operations.hashes;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.math.BigDecimal;
import java.util.List;

@RedisCommand("hincrbyfloat")
class HIncrByFloat extends HIncrBy {
    HIncrByFloat(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice hsetValue(Slice key1, Slice key2, Slice value) {
        BigDecimal numericValue = new BigDecimal(value.toString());
        Slice foundValue = base().getSlice(key1, key2);
        if (foundValue != null) {
            numericValue = numericValue.add(new BigDecimal((new String(foundValue.data()))));
        }
        String data = String.valueOf(BigDecimal.valueOf(numericValue.intValue()).compareTo(numericValue) == 0
                ? numericValue.intValue() : numericValue);

        Slice res = Slice.create(data);
        base().putSlice(key1, key2, res, -1L);

        return Response.bulkString(res);
    }
}
