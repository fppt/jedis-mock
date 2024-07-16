package com.github.fppt.jedismock.operations.keys;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.EnumSet;
import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToLong;

@RedisCommand("pexpire")
class PExpire extends AbstractRedisOperation {
    enum Options {
        XX, NX, LT, GT
    }

    private final EnumSet<Options> options = EnumSet.noneOf(Options.class);

    PExpire(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    long getValue(List<Slice> params) {
        return convertToLong(new String(params.get(1).data()));
    }

    @Override
    protected int minArgs() {
        return 2;
    }

    protected Slice response() {
        param:
        for (int i = 2; i < params().size(); i++) {
            String opt = params().get(i).toString();
            for (Options value : Options.values()) {
                if (value.toString().equalsIgnoreCase(opt)) {
                    options.add(value);
                    continue param;
                }
            }
            return Response.error("ERR Unsupported option " + opt);
        }
        if (options.contains(Options.NX) && options.size() > 1) {
            return Response.error("ERR NX and XX, GT or LT options at the same time are not compatible");
        }
        if (options.contains(Options.GT) && options.contains(Options.LT)) {
            return Response.error("ERR GT and LT options at the same time are not compatible");
        }

        final long newTTL;
        try {
            newTTL = getValue(params());
            //Check for potential overflow
            Math.addExact(newTTL, System.currentTimeMillis());
        } catch (ArithmeticException e) {
            return Response.error(
                    String.format("ERR invalid expire time in '%s' command",
                            self().value()));
        }

        Slice key = params().get(0);
        boolean allow = base().exists(key);
        Long oldTTL = base().getTTL(params().get(0));
        if (oldTTL == null) oldTTL = -2L;
        if (options.contains(Options.NX)) allow = allow && oldTTL < 0;
        if (options.contains(Options.XX)) allow = allow && oldTTL >= 0;
        if (options.contains(Options.LT) && oldTTL >= 0) allow = allow && newTTL < oldTTL;
        if (options.contains(Options.GT)) allow = allow && oldTTL >= 0 && newTTL > oldTTL;
        if (allow) {
            return Response.integer(base().setTTL(key, newTTL));
        } else return Response.integer(0);
    }
}
