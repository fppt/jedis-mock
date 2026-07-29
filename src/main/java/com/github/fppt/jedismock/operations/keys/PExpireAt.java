package com.github.fppt.jedismock.operations.keys;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.keys.paramsparser.ExpirationExtraParam;
import com.github.fppt.jedismock.operations.keys.paramsparser.ExpirationParamsException;
import com.github.fppt.jedismock.operations.keys.paramsparser.ExpirationTimeParam;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.KeyspaceEvent;
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
                if (newDeadline <= base().getClock().millis()) {
                    //A deadline in the past deletes the key immediately, and is
                    //reported as a 'del' rather than an 'expired'
                    base().deleteValue(key);
                    base().notifyKeyspaceEvent(KeyspaceEvent.DEL, key);
                    return Response.integer(1);
                }
                long result = base().setDeadline(key, newDeadline);
                base().notifyKeyspaceEvent(KeyspaceEvent.EXPIRE, key);
                return Response.integer(result);
            } else return Response.integer(0);
        } catch (ExpirationParamsException e) {
            return Response.error(e.getMessage());
        }
    }
}
