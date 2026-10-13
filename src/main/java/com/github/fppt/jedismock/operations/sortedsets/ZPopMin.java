package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand(value = "zpopmin", arity = -2, readOnly = false)
public class ZPopMin extends ZPop {

    ZPopMin(RedisBase base, List<Slice> params) {
        super(base, params, false);
    }

}
