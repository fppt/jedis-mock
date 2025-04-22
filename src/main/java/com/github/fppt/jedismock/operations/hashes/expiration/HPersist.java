package com.github.fppt.jedismock.operations.hashes.expiration;

import com.github.fppt.jedismock.datastructures.RMHash;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.keys.paramsparser.ExpirationFieldsParam;
import com.github.fppt.jedismock.operations.keys.paramsparser.ExpirationParamsException;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;

@RedisCommand("hpersist")
public class HPersist extends AbstractRedisOperation {
    public HPersist(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        try {
            Slice key = params().get(0);
            ExpirationFieldsParam fieldsParam = new ExpirationFieldsParam(
                    params(), 1
            );
            List<Slice> response = new ArrayList<>();
            RMHash hash = base().getHash(key);
            if (hash != null) {
                for (Slice field : fieldsParam.getFields()) {
                    if (!hash.keyExists(field)) {
                        response.add(Response.integer(-2L));
                    } else {
                        Long oldDeadline = hash.getDeadline(field);
                        if (oldDeadline != null && oldDeadline > 0) {
                            response.add(Response.integer(hash.setDeadline(field, -1)));
                        } else {
                            response.add(Response.integer(-1L));
                        }
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
}
