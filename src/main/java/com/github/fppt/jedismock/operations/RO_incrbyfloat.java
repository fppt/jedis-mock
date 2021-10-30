package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.List;

@RedisCommand("incrbyfloat")
class RO_incrbyfloat extends RO_incrOrDecrByFloat {
    RO_incrbyfloat(RedisBase base, List<Slice> params) {
        super(base, params);
    }
}
