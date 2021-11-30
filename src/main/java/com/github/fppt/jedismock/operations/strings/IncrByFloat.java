package com.github.fppt.jedismock.operations.strings;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.List;

@RedisCommand("incrbyfloat")
class IncrByFloat extends IncrOrDecrByFloat {
    IncrByFloat(RedisBase base, List<Slice> params) {
        super(base, params);
    }
}
