package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

@RedisCommand("spop")
class RO_spop extends RO_pop<Set<Slice>> {
    RO_spop(RedisBase base, List<Slice> params ) {
        super(base, params);
    }

    @Override
    Slice popper(Set<Slice> collection) {
        Iterator<Slice> it = collection.iterator();
        Slice v = it.next();
        it.remove();
        return v;
    }

    @Override
    Set<Slice> getDataFromBase(Slice key) {
        final RMSet setDBObj = getSetFromBase(key);
        return setDBObj.getStoredData();
    }
}
