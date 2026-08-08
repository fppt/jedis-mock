package com.github.fppt.jedismock.operations.strings;

import com.github.fppt.jedismock.datastructures.RMString;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.KeyspaceEvent;
import com.github.fppt.jedismock.storage.RedisBase;

import java.math.BigDecimal;
import java.util.List;

abstract class IncrOrDecrByFloat extends AbstractRedisOperation {
    IncrOrDecrByFloat(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int minArgs() {
        return 1;
    }

    private static BigDecimal toBigDecimal(String value) {
        if (value != null) {
            if (value.toLowerCase().endsWith("inf")) {
                throw new NumberFormatException("ERR increment would produce NaN or Infinity");
            } else try {
                return new BigDecimal(value);
            } catch (NumberFormatException e) {
                NumberFormatException modified = new NumberFormatException("ERR value is not a valid float");
                modified.initCause(e);
                throw modified;
            }
        } else {
            throw new NumberFormatException("ERR value is not a valid float");
        }
    }

    protected Slice response() {
        Slice key = params().get(0);
        BigDecimal numericValue = toBigDecimal(params().get(1).toString());

        RMString foundValue = base().getRMString(key);
        if (foundValue != null) {
            numericValue = numericValue.add(toBigDecimal(foundValue.getStoredDataAsString()));
        }

        String data = String.valueOf(BigDecimal.valueOf(numericValue.intValue()).compareTo(numericValue) == 0
                ? numericValue.intValue() : numericValue);

        RMString res = RMString.create(data);
        base().putValue(key, res);
        base().notifyKeyspaceEvent(KeyspaceEvent.INCRBYFLOAT, key);

        return Response.bulkString(res.getAsSlice());
    }
}
