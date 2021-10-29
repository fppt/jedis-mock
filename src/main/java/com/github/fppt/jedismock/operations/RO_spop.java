package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.server.Slice;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

@TxOperation("spop")
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
}
