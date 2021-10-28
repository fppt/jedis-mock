package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.stream.Collectors;

class RO_set extends AbstractRedisOperation {
    private final List<String> additionalParams;

    RO_set(RedisBase base, List<Slice> params) {
        super(base, params);
        additionalParams = params()
                .stream().skip(2).map(Slice::toString).collect(Collectors.toList());
    }

    Slice response() {
        Slice key = params().get(0);
        Slice value = params().get(1);

        if (nx()) {
            Slice old = base().getValue(key);
            if (old == null) {
                base().putValue(key, value, ttl());
                return Response.OK;
            } else {
                return Response.NULL;
            }
        } else if (xx()) {
            Slice old = base().getValue(key);
            if (old == null) {
                return Response.NULL;
            } else {
                base().putValue(key, value, ttl());
                return Response.OK;
            }
        } else {
            base().putValue(key, value, ttl());
            return Response.OK;
        }
    }

    private boolean nx() {
        return additionalParams.stream().anyMatch("nx"::equalsIgnoreCase);
    }

    private boolean xx() {
        return additionalParams.stream().anyMatch("xx"::equalsIgnoreCase);
    }

    private Long ttl() {
        String previous = null;
        for (String param : additionalParams) {
            if ("ex".equalsIgnoreCase(previous)) {
                return 1000 * Utils.convertToLong(param);
            } else if ("px".equalsIgnoreCase(previous)) {
                return Utils.convertToLong(param);
            }
            previous = param;
        }
        return null;
    }

}
