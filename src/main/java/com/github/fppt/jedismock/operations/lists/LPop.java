package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("lpop")
class LPop extends ListPopper {
    LPop(RedisBase base, List<Slice> params ) {
        super(base, params);
    }

    Slice popper(List<Slice> list) {
        return list.remove(0);
    }
}
