package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("llen")
class LLen extends AbstractRedisOperation {
    LLen(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        Slice key = params().get(0);
        RMList listDBObj = getListFromBaseOrCreateEmpty(key);
        List<Slice> list = listDBObj.getStoredData();
        return Response.integer(list.size());
    }
}
