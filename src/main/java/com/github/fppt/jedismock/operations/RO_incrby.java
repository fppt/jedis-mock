package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.server.Slice;

import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToLong;

@TxOperation("incrby")
class RO_incrby extends RO_incrOrDecrBy {
    RO_incrby(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    long incrementOrDecrementValue(List<Slice> params){
        return convertToLong(String.valueOf(params.get(1)));
    }
}
