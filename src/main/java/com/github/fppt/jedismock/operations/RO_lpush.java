package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.List;

class RO_lpush extends RO_add {
    RO_lpush(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    void addSliceToList(List<Slice> list, Slice slice) {
        list.add(0, slice);
    }
}
