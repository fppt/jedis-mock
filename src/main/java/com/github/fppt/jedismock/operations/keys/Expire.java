package com.github.fppt.jedismock.operations.keys;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.List;

@RedisCommand("expire")
class Expire extends PExpire {
    Expire(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected boolean useMillis() {
        return false;
    }
}
