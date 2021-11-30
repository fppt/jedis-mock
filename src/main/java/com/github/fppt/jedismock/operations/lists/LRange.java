package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToInteger;

@RedisCommand("lrange")
class LRange extends AbstractRedisOperation {
    LRange(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        Slice key = params().get(0);
        RMList listDBObj = getListFromBaseOrCreateEmpty(key);
        List<Slice> list = listDBObj.getStoredData();

        int start = convertToInteger(params().get(1).toString());
        int end = convertToInteger(params().get(2).toString());

        if (start < 0) {
            start = list.size() + start;
            if (start < 0) {
                start = 0;
            }
        }
        if (end < 0) {
            end = list.size() + end;
            if (end < 0) {
                end = 0;
            }
        }
        List<Slice> result = new ArrayList<>();
        for (int i = start; i <= end && i < list.size(); i++) {
            result.add(Response.bulkString(list.get(i)));
        }
        return Response.array(result);
    }
}
