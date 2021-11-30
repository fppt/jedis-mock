package com.github.fppt.jedismock.operations.strings;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToLong;

@RedisCommand("psetex")
class PSetEx extends SetEx {
    PSetEx(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    long timeoutToSet(List<Slice> params){
        return convertToLong(new String(params.get(1).data()));
    }
}
