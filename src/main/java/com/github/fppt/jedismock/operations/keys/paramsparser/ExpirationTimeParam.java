package com.github.fppt.jedismock.operations.keys.paramsparser;

import com.github.fppt.jedismock.datastructures.Slice;

public final class ExpirationTimeParam {
    private final long millis;

    public ExpirationTimeParam(String commandName,
                        Slice param,
                        boolean useMillis,
                        long timestampToCheckOverflow) throws ExpirationParamsException {
        long value;
        try {
            value = Long.parseLong(new String(param.data()));
        } catch (NumberFormatException e) {
            throw new ExpirationParamsException("ERR value is not an integer or out of range");
        }
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
