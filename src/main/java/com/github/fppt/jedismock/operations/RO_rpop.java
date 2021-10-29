package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.List;

class RO_rpop extends RO_pop<List<Slice>> {
    RO_rpop(RedisBase base, List<Slice> params ) {
        super(base, params);
    }

    @Override
    Slice popper(List<Slice> list) {
        return list.remove(list.size() - 1);
    }

    @Override
    List<Slice> getDataFromBase(Slice key) {
        final RMList listDBObj = getListFromBase(key);
        return listDBObj.getStoredData();
    }
}
