package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.WrongValueTypeException;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;

abstract class ListPopper extends AbstractRedisOperation {
    ListPopper(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    abstract Slice popper(List<Slice> list);

    protected final Slice response() {
        Slice key = params().get(0);
        final RMList listDBObj = getListFromBaseOrCreateEmpty(key);
        final List<Slice> list = listDBObj.getStoredData();
        if (list.isEmpty()) return Response.NULL;
        if (params().size() > 1) {
            //Count param
            Slice countParam = params().get(1);
            int count = Integer.parseInt(countParam.toString());
            if (count <= 0) {
                throw new WrongValueTypeException("value is out of range, must be positive");
            }
            List<Slice> responseList = new ArrayList<>();
            while (count > 0 && list.size() > 0) {
                responseList.add(Response.bulkString(popper(list)));
                count--;
            }
            return Response.array(responseList);
        } else {
            return Response.bulkString(popper(list));
        }
    }

}
