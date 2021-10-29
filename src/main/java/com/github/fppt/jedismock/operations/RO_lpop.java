package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.server.Slice;

import java.util.LinkedList;
import java.util.List;

@TxOperation("lpop")
class RO_lpop extends RO_pop<LinkedList<Slice>> {
    RO_lpop(RedisBase base,List<Slice> params ) {
        super(base, params);
    }

    @Override
    Slice popper(LinkedList<Slice> list) {
        return list.removeFirst();
    }
}
