package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

import static com.github.fppt.jedismock.Utils.serializeObject;

abstract class RO_add extends AbstractRedisOperation {
    RO_add(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    abstract void addSliceToList(List<Slice> list, Slice slice);

    Slice response() {
        Slice key = params().get(0);
        final RMList listDBObj = getListFromBase(key);
        final List<Slice> list = listDBObj.getStoredData();

        for (int i = 1; i < params().size(); i++) {
            addSliceToList(list, params().get(i));
        }

        try {
            base().putSlice(key, serializeObject(list));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        return Response.integer(list.size());
    }
}
