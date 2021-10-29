package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.server.Slice;

import java.util.List;

@TxOperation("incrbyfloat")
class RO_incrbyfloat extends RO_incrOrDecrByFloat {
    RO_incrbyfloat(RedisBase base, List<Slice> params) {
        super(base, params);
    }
}
