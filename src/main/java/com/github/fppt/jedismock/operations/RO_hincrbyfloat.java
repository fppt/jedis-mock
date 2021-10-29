package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.math.BigDecimal;
import java.util.List;

@TxOperation("hincrbyfloat")
class RO_hincrbyfloat extends RO_hincrby {
    RO_hincrbyfloat(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice hsetValue(Slice key1, Slice key2, Slice value) {
        BigDecimal numericValue = new BigDecimal(value.toString());
        Slice foundValue = base().getValue(key1, key2);
        if (foundValue != null) {
            numericValue = numericValue.add(new BigDecimal((new String(foundValue.data()))));
        }
        String data = String.valueOf(BigDecimal.valueOf(numericValue.intValue()).compareTo(numericValue) == 0
                ? numericValue.intValue() : numericValue);

        Slice res = Slice.create(data);
        base().putValue(key1, key2, res, -1L);

        return Response.bulkString(res);
    }
}
