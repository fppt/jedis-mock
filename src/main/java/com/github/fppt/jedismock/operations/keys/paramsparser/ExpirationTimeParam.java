package com.github.fppt.jedismock.operations.keys.paramsparser;

import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.OptionalLong;

public final class ExpirationTimeParam {
    private final long millis;

    public ExpirationTimeParam(String commandName,
                        Slice param,
                        boolean useMillis,
                        long timestampToCheckOverflow) throws ExpirationParamsException {
        OptionalLong parsed = Utils.parseRedisLong(new String(param.data()));
        if (!parsed.isPresent()) {
            throw new ExpirationParamsException("ERR value is not an integer or out of range");
        }
        long value = parsed.getAsLong();
        try {
            millis = useMillis ? value : Math.multiplyExact(value, 1000L);
            Math.addExact(millis, timestampToCheckOverflow);
        } catch (ArithmeticException e) {
            throw new ExpirationParamsException(String.format("ERR invalid expire time in '%s' command",
                    commandName));
        }
    }

    public long getMillis() {
        return millis;
    }
}
