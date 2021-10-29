package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToLong;

class RO_psetex extends RO_setex {
    RO_psetex(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    long valueToSet(List<Slice> params){
        return convertToLong(new String(params.get(1).data()));
    }
}
