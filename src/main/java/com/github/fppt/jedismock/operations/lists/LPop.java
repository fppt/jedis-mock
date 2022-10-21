package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.ArrayList;
import java.util.List;

@RedisCommand("lpop")
class LPop extends AbstractRedisOperation {
    LPop(RedisBase base, List<Slice> params ) {
        super(base, params);
    }

    Slice popper(List<Slice> list) {
        return list.remove(0);
    }

    protected Slice response() {
        Slice key = params().get(0);
        final RMList listDBObj = getListFromBaseOrCreateEmpty(key);
        List<Slice> list = listDBObj.getStoredData();
        if(list == null || list.isEmpty()) return Response.NULL;

        base().putValue(key, listDBObj);

        if (params().size() == 2) {
            Slice countParam = params().get(1);
            Integer count = Integer.decode(countParam.toString());
            count = count <= list.size() ? count : list.size();
            List<Slice> responseList = new ArrayList<>();
            for (int i=0; i < count; ++i) {
                Slice value = list.remove(0);
                if (value != null) {
                    responseList.add(Response.bulkString(value));
                } else {
                    break;
                }
            }
            return Response.array(responseList);
        } else {
            Slice v = popper(list);
            return Response.bulkString(v);
        }
    }
}
