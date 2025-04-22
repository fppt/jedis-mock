package com.github.fppt.jedismock.operations.keys;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("expireat")
class ExpireAt extends PExpireAt {
    ExpireAt(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected boolean useMillis() {
        return false;
    }
}
