package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

class RO_blpop extends RO_bpop {
    RO_blpop(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    RO_pop popper(List<Slice> params) {
        return new RO_lpop(base(), params);
    }

    @Override
    List<Slice> getDataFromBase(Slice key) {
        final RMList listDBObj = getListFromBase(key);
        return listDBObj.getStoredData();
    }
}
