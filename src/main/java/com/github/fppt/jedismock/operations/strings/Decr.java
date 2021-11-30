package com.github.fppt.jedismock.operations.strings;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.List;

@RedisCommand("decr")
class Decr extends DecrBy {
    Decr(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    long incrementOrDecrementValue(List<Slice> params){
        return -1L;
    }
}
