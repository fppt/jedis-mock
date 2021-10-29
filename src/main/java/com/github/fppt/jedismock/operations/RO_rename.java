package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.Map;

@TxOperation("rename")
class RO_rename extends AbstractRedisOperation {

    RO_rename(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    private boolean rename(Slice key, Slice newKey) {
        Map<Slice, Slice> value = base().getFieldsAndValues(key);
        final Long ttl = base().getTTL(key);
        if (ttl == null) {
            return false;
        }

        base().deleteValue(newKey);
        for (Map.Entry<Slice, Slice> entry : value.entrySet()) {
            base().putValue(newKey, entry.getKey(), entry.getValue(), ttl);
        }
        base().deleteValue(key);
        return true;
    }

    @Override
    Slice response() {
        final Slice key = params().get(0);
        final Slice newKey = params().get(1);
        if (!rename(key, newKey)) {
            return Response.error("ERR no such key");
        }
        return Response.OK;
    }
}
