package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.List;

@RedisCommand("expire")
class RO_expire extends RO_pexpire {
    RO_expire(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    long getValue(List<Slice> params){
        return super.getValue(params) * 1000;
    }
}
