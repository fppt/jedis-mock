package com.github.fppt.jedismock.operations.strings;

import com.github.fppt.jedismock.datastructures.RMString;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToLong;

@RedisCommand("setex")
class SetEx extends Set {
    SetEx(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    long timeoutToSet(List<Slice> params){
        return convertToLong(new String(params.get(1).data())) * 1000;
    }

    protected Slice response() {
        base().putValue(params().get(0), params().get(2).extract(), timeoutToSet(params()));
        return Response.OK;
    }
}
