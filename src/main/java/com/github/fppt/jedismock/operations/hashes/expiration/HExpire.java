package com.github.fppt.jedismock.operations.hashes.expiration;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("hexpire")
public class HExpire extends HPExpire {
    public HExpire(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected boolean useMillis() {
        return false;
    }
}
