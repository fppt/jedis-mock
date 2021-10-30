package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToInteger;

@RedisCommand("lindex")
class RO_lindex extends AbstractRedisOperation {
    RO_lindex(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        Slice key = params().get(0);
        RMList listDBObj = getListFromBase(key);
        List<Slice> list = listDBObj.getStoredData();
        if(list.isEmpty()) return Response.NULL;

        int index = convertToInteger(params().get(1).toString());
        if (index < 0) {
            index = list.size() + index;
            if (index < 0) {
                return Response.NULL;
            }
        }
        if (index >= list.size()) {
            return Response.NULL;
        }
        return Response.bulkString(list.get(index));
    }
}
