package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("zpopmin")
public class ZPopMin extends ZPop {

    ZPopMin(RedisBase base, List<Slice> params) {
        super(base, params, false);
    }
}
