package com.github.fppt.jedismock.operations.keys;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.keys.paramsparser.ExpirationExtraParam;
import com.github.fppt.jedismock.operations.keys.paramsparser.ExpirationParamsException;
import com.github.fppt.jedismock.operations.keys.paramsparser.ExpirationTimeParam;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("pexpireat")
class PExpireAt extends AbstractRedisOperation {
    PExpireAt(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int minArgs() {
        return 2;
    }

    protected boolean useMillis() {
        return true;
    }

    protected Slice response() {
        try {
            Slice key = params().get(0);
            ExpirationTimeParam expirationTime = new ExpirationTimeParam(self().value(),
                    params().get(1), useMillis(), 0);
            ExpirationExtraParam extraParam = new ExpirationExtraParam(
                    params(), false
            );
            long newDeadline = expirationTime.getMillis();
            if (base().exists(key) && extraParam.checkTiming(
                    base().getDeadline(key), newDeadline)) {
                return Response.integer(base().setDeadline(key, newDeadline));
            } else return Response.integer(0);
        } catch (ExpirationParamsException e) {
            return Response.error(e.getMessage());
        }
    }
}
