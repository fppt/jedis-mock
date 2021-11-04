package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.Collections;
import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToInteger;
import static java.util.stream.Collectors.toList;

@RedisCommand("lrange")
class RO_lrange extends AbstractRedisOperation {
    RO_lrange(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        Slice key = params().get(0);
        RMList listDBObj = getListFromBase(key);
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
        return Response.array(Collections.unmodifiableList(list.stream().skip(start).limit(Math.max(end - start + 1, 0)).map(Response::bulkString).collect(toList())));
    }
}
