package com.github.fppt.jedismock.operations.strings;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToLong;

@RedisCommand("incrby")
class IncrBy extends IncrOrDecrBy {
    IncrBy(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    long incrementOrDecrementValue(List<Slice> params){
        return convertToLong(String.valueOf(params.get(1)));
    }
}
