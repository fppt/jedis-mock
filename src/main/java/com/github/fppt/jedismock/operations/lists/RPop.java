package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.List;

@RedisCommand("rpop")
class RPop extends AbstractRedisOperation {
    RPop(RedisBase base, List<Slice> params ) {
        super(base, params);
    }

    Slice popper(List<Slice> list) {
        return list.remove(list.size() - 1);
    }

    protected Slice response() {
        Slice key = params().get(0);
        final RMList listDBObj = getListFromBaseOrCreateEmpty(key);
        final List<Slice> list = listDBObj.getStoredData();
        if(list == null || list.isEmpty()) return Response.NULL;
        Slice v = popper(list);
        base().putValue(key, listDBObj);
        return Response.bulkString(v);
    }

}
