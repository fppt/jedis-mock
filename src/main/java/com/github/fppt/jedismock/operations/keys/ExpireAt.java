package com.github.fppt.jedismock.operations.keys;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand(value = "expireat", arity = -3, readOnly = false)
class ExpireAt extends PExpireAt {
    ExpireAt(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected boolean useMillis() {
        return false;
    }
}
