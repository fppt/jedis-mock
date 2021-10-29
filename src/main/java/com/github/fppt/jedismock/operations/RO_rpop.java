package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.server.Slice;

import java.util.LinkedList;
import java.util.List;

@TxOperation("rpop")
class RO_rpop extends RO_pop<LinkedList<Slice>> {
    RO_rpop(RedisBase base, List<Slice> params ) {
        super(base, params);
    }

    @Override
    Slice popper(LinkedList<Slice> list) {
        return list.removeLast();
    }
}
