package com.github.fppt.jedismock.operations.strings;

import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.datastructures.RMDataStructure;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

@RedisCommand("set")
class Set extends AbstractRedisOperation {
    private final List<String> additionalParams;

    Set(RedisBase base, List<Slice> params) {
        super(base, params);
        additionalParams = params()
                .stream().skip(2).map(Slice::toString).collect(Collectors.toList());
    }

    @Override
    protected int minArgs() {
        return 2;
    }

    protected Slice response() {
        Slice key = params().get(0);
        Slice value = params().get(1);
        BiConsumer<Slice, RMDataStructure> valueSetter;
        try {
            valueSetter = valueSetter();
        } catch (IllegalArgumentException e) {
            return Response.error(e.getMessage());
        }
        if (nx()) {
            Slice old = base().getSlice(key);
            if (old == null) {
                valueSetter.accept(key, value.extract());
                return Response.OK;
            } else {
                return Response.NULL;
            }
        } else if (xx()) {
            Slice old = base().getSlice(key);
            if (old == null) {
                return Response.NULL;
            } else {
                valueSetter.accept(key, value.extract());
                return Response.OK;
            }
        } else {
            valueSetter.accept(key, value.extract());
            return Response.OK;
        }

    }

    private boolean nx() {
        return additionalParams.stream().anyMatch("nx"::equalsIgnoreCase);
    }

    private boolean xx() {
        return additionalParams.stream().anyMatch("xx"::equalsIgnoreCase);
    }

    private boolean keepTTL() {
        return additionalParams.stream().anyMatch("keepttl"::equalsIgnoreCase);
    }

    private long parseAndValidate(String param, int multiplier) {
        long value = Utils.convertToLong(param);
        if (value <= 0) {
            throw new IllegalArgumentException(String.format(
                    "ERR invalid expire time in '%s' command", self().value()));
        }
        try {
            value = Math.multiplyExact(multiplier, value);
            Math.addExact(base().getClock().millis(), value);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(String.format(
                    "ERR invalid expire time in '%s' command", self().value()));
        }
        return value;
    }

    private BiConsumer<Slice, RMDataStructure> valueSetter() {
        String previous = null;
        for (String param : additionalParams) {
            if ("ex".equalsIgnoreCase(previous)) {
                long ex = parseAndValidate(param, 1000);
                return (k, v) -> base().putValue(k, v, ex);
            } else if ("px".equalsIgnoreCase(previous)) {
                long px = parseAndValidate(param, 1);
                return (k, v) -> base().putValue(k, v, px);
            } else if ("exat".equalsIgnoreCase(previous)) {
                long exat = parseAndValidate(param, 1000);
                return (k, v) -> {
                    base().putValue(k, v);
                    base().setDeadline(k, exat);
                };
            } else if ("pxat".equalsIgnoreCase(previous)) {
                long pxat = parseAndValidate(param, 1);
                return (k, v) -> {
                    base().putValue(k, v);
                    base().setDeadline(k, pxat);
                };
            }
            previous = param;
        }
        if (keepTTL()) {
            return (k, v) -> {
                Long deadline = base().getDeadline(k);
                base().putValue(k, v);
                base().setDeadline(k, deadline);
            };
        } else {
            return (k, v) -> base().putValue(k, v);
        }
    }
}
