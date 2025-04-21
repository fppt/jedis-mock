package com.github.fppt.jedismock.operations.hashes.expiration;

import com.github.fppt.jedismock.datastructures.RMHash;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.keys.paramsparser.ExpirationExtraParam;
import com.github.fppt.jedismock.operations.keys.paramsparser.ExpirationFieldsParam;
import com.github.fppt.jedismock.operations.keys.paramsparser.ExpirationParamsException;
import com.github.fppt.jedismock.operations.keys.paramsparser.ExpirationTimeParam;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;

@RedisCommand("hpexpire")
public class HPExpire extends AbstractRedisOperation {
    public HPExpire(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected boolean useMillis() {
        return true;
    }

    @Override
    protected Slice response() {
        try {
            Slice key = params().get(0);
            ExpirationTimeParam expirationTime = new ExpirationTimeParam(self().value(),
                    params().get(1), useMillis(), base().getClock().millis());
            ExpirationExtraParam extraParam = new ExpirationExtraParam(
                    params(), true
            );
            ExpirationFieldsParam fieldsParam = new ExpirationFieldsParam(
                    params(), extraParam.getIndex()
            );
            long newTTL = expirationTime.getMillis();
            List<Slice> response = new ArrayList<>();
            RMHash hash = base().getHash(key);
            if (hash != null) {
                for (Slice field : fieldsParam.getFields()) {
                    if (!hash.keyExists(field)) {
                        response.add(Response.integer(-2L));
                    } else if (extraParam.checkTiming(
                            hash.getTTL(field), newTTL)) {
                        response.add(Response.integer(hash.setTTL(field, newTTL)));
                    } else {
                        response.add(Response.integer(0));
                    }
                }
            } else {
                for (int i = 0; i < fieldsParam.getFields().size(); i++) {
                    response.add(Response.integer(-2L));
                }
            }
            return Response.array(response);
        } catch (ExpirationParamsException e) {
            return Response.error(e.getMessage());
        }
    }

    @Override
    protected int minArgs() {
        return 5;
    }
}
