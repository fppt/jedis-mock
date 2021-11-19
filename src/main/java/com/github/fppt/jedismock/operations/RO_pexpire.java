package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.stream.Collectors;

import static com.github.fppt.jedismock.Utils.convertToLong;

@RedisCommand("pexpire")
class RO_pexpire extends AbstractRedisOperation {
    private final List<String> additionalParams;

    RO_pexpire(RedisBase base, List<Slice> params) {
        super(base, params);
        additionalParams = params()
                .stream().skip(2).map(Slice::toString).collect(Collectors.toList());
    }

    long getValue(List<Slice> params){
        return convertToLong(new String(params.get(1).data()));
    }

    Slice response() {
        Slice key = params().get(0);
        long value = getValue(params());

        Slice old = base().getSlice(key);
        if (nx() && old != null || xx() && old == null) {
            return Response.integer(0);
        }

        long oldTTL = base().getTTL(key);
        if (gt() &&  (oldTTL == -1 || oldTTL >= value) || lt() && oldTTL != -1 && oldTTL <= value) {
            return Response.integer(0);
        }

        return Response.integer(base().setTTL(key, value));
    }

    private boolean nx() {
        return additionalParams.stream().anyMatch("nx"::equalsIgnoreCase);
    }

    private boolean xx() {
        return additionalParams.stream().anyMatch("xx"::equalsIgnoreCase);
    }

    private boolean gt() {
        return additionalParams.stream().anyMatch("gt"::equalsIgnoreCase);
    }

    private boolean lt() {
        return additionalParams.stream().anyMatch("lt"::equalsIgnoreCase);
    }

}
